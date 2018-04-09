#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import enum
import http
import io

import aiohttp
import aiohttp.web
import jsonschema
import multidict

import pagent.agent_service
import prpc

from . import common, jobs


PROXY_EXCEPTION_TIMEOUT = 5
WS_PROXY_EVENT_QUEUE_DEPTH = 32

ROUTE_VARIABLE_PATH = 'path'


SCHEMA_PROXY_ACTIVE = {
    'type': 'object',
    'properties': {
        'url': {'type': 'string'}
    },
    'required': ['url'],
    'additionalProperties': False
}


@enum.unique
class WSMessageDirection(enum.Enum):
    """Websocket message direction for event queue."""
    JOB_TO_CLIENT = enum.auto()
    CLIENT_TO_JOB = enum.auto()


async def proxy_passive(request):
    """Forward HTTP/WS requests to job running under agent.

    Automatically detects WS connection by headers.
    """
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    # Path is already decoded there.
    path = '/' + request.match_info[ROUTE_VARIABLE_PATH]

    connection = request.app[common.KEY_CONN_MANAGER].connection(conn_uid)

    ws_response = aiohttp.web.WebSocketResponse()
    if ws_response.can_prepare(request):
        return await _proxy_websocket(
            request, ws_response, connection, job_uid, path
        )
    else:
        return await _proxy_http(request, connection, job_uid, path)


async def proxy_active(request):
    """Establish a WS bridge connection between job and a given endpoint."""
    assert request.method == 'POST'
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    # Path is already decoded there.
    path = '/' + request.match_info[ROUTE_VARIABLE_PATH]

    # TODO: We can support additional WS connection features like subprotocols,
    # heartbeat etc if needed. They should go into this config.
    payload = await request.json()
    jsonschema.validate(payload, SCHEMA_PROXY_ACTIVE)

    connection = request.app[common.KEY_CONN_MANAGER].connection(conn_uid)
    remote_ws_established = request.app.loop.create_future()
    request.app.loop.create_task(
        _proxy_active(
            payload['url'],
            connection,
            job_uid,
            path,
            list(request.query.items()),
            list(request.headers.items()),
            remote_ws_established
        )
    )

    try:
        connected = await remote_ws_established
        assert connected
        return aiohttp.web.Response()
    except prpc.RpcError as ex:
        return aiohttp.web.Response(
            status=http.HTTPStatus.BAD_REQUEST, text=str(ex)
        )


async def _proxy_active(url, connection, job_uid, path, query, headers,
                        remote_ws_established):
    """Establishes an agent-side WS connection, reports back to the caller,
    try to establish a WS tunnel to a given url.
    """
    call = connection.call_bistream(
        pagent.agent_service.AgentService.ws_connect.__name__,
        [job_uid, path, query, headers]
    )

    try:
        async with call:
            connected = await call.stream.receive()
            if not connected:
                # Should actually raise...
                await call.result
                # ...but deadlock safety first.
                remote_ws_established.set_exception(
                    RuntimeError('failed ws_connect call did not raise')
                )
                return
            else:
                remote_ws_established.set_result(True)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as websocket:
                    return await _proxy_websocket_events(
                        call, websocket, connection.loop
                    )
    except Exception as ex:
        if not remote_ws_established.done():
            remote_ws_established.set_exception(ex)


async def _proxy_http(request, connection, job_uid, path):
    """Proxies the HTTP request.

    Args:
        request: aiohttp request instance.
        connection: prpc connection with the agent.
        job_uid: Target job uid.
        path: HTTP path.
    """
    call = connection.call_bistream(
        pagent.agent_service.AgentService.http_request.__name__,
        [
            job_uid, request.method, path,
            list(request.query.items()),
            list(request.headers.items())
        ]
    )
    async with call:
        response_writer = request.app.loop.create_task(
            _proxy_http_forward_response(request, call)
        )
        try:
            while True:
                chunk = await request.content.readany()
                if call.stream.is_closed:
                    break
                await call.stream.send(chunk)
                if not chunk:
                    break
            response = await response_writer
            return response
        finally:
            if not response_writer.done():
                response_writer.cancel()


async def _proxy_http_forward_response(request, call):
    """Forward agent proxy response to an HTTP stream.

    Must be run in parallel with the sending task.

    Swallows any RPC exceptions, transforming them to HTTP error pages.

    Args:
        request: aiohttp request instance.
        call: Active prpc call to proxy endpoint.

    Returns:
        aiohttp.Web.StreamResponse instance.
    """
    response = aiohttp.web.StreamResponse()
    msg_index = 0
    async for msg in call.stream:
        if msg_index == 0:
            response.set_status(msg)
        elif msg_index == 1:
            headers = multidict.MultiDict(msg)
            # Drop cache-related headers from the response, if any.
            # Dynamic proxy content shouldn't be cached.
            headers.popall('Cache-Control', None)
            headers.popall('Expires', None)
            response.headers.update(headers)
            # TODO: add proxy headers?
            # (X-Forwarded-For, drop Host => to X-Forwarded-Host,
            #  X-Forwarded-Proto OR the new shiny 'Forwarded')
            # Note:
            #   All of those require 'append' like behavior.
            _proxy_http_setup_encoding(response)
            await response.prepare(request)
        else:
            await response.write(msg)
        msg_index += 1

    # Incomplete response.
    if msg_index < 2:
        return await _proxy_error_response(call)

    await response.write_eof()
    return response


def _proxy_http_setup_encoding(response):
    """Prepare Content-/Transfer- Encoding headers so that they
    correspond to actual response content.

    Args:
        response: aiohttp.web.StreamResponse instance.

    Note:
        Response must be not yet 'prepared'.
    """
    assert not response.prepared
    # Agent proxy backend sends us decompressed stream for now,
    # so this header should never present.
    response.headers.popall('Content-Encoding', None)
    # However, we can compress it back if client allows it.
    response.enable_compression()
    # Enable chunked encoding if needed.
    chunked_response = (
        response.headers.get('Transfer-Encoding', '').lower() == 'chunked' or
        response.headers.get('Content-Length', None) is None
    )
    if chunked_response:
        response.headers.popall('Content-Length', None)
        response.headers.popall('Transfer-Encoding', None)
        response.enable_chunked_encoding()


async def _proxy_websocket(request, websocket, connection, job_uid, path):
    """Proxify a websocket connection.

    Args:
        request: aiohttp request.
        websocket: aiohttp.web.WebSocketResponse instance.
        connection: prpc connection to a proper agent.
        job_uid: Job id that contains a target server.
        path: HTTP path.
    """
    call = connection.call_bistream(
        pagent.agent_service.AgentService.ws_connect.__name__,
        [
            job_uid, path,
            list(request.query.items()),
            list(request.headers.items())
        ]
    )
    async with call:
        connected = await call.stream.receive()
        if not connected:
            return await _proxy_error_response(call)
        await websocket.prepare(request)
        return await _proxy_websocket_events(call, websocket, request.app.loop)


async def _proxy_websocket_events(call, websocket, loop):
    """Route websocket events between pRpc call and actual local websocket.

    Args:
        call: pRpc call.
        websocket: Client or server websocket object from aiohttp.
        loop: asyncio event loop.
    """
    # Use event queue to merge to event streams
    # (messages from/to client).
    #
    # Queue is significantly faster than
    #     asyncio.wait(
    #         [event_source_1, event_source_2], return_when=FIRST_COMPLETED
    #     )
    # as it does not create new Tasks and async context switches.
    #
    # Websockets connections are likely to carry a lot of small messages,
    # so this optmization is pretty important (~3x speedup).
    event_queue = asyncio.Queue(WS_PROXY_EVENT_QUEUE_DEPTH)
    ws_listen_task = loop.create_task(
        _proxy_websocket_listen_ws(websocket, event_queue)
    )
    stream_listen_task = loop.create_task(
        _proxy_websocket_listen_stream(call.stream, event_queue)
    )
    try:
        while True:
            direction, data = await event_queue.get()
            if data is None:
                break
            if direction == WSMessageDirection.CLIENT_TO_JOB:
                await call.stream.send(data)
            elif direction == WSMessageDirection.JOB_TO_CLIENT:
                if isinstance(data, str):
                    await websocket.send_str(data)
                elif isinstance(data, bytes):
                    await websocket.send_bytes(data)
                else:
                    break
    except Exception:
        # Any errors just close both sockets.
        pass
    finally:
        await websocket.close()
        await call.stream.close()
    await asyncio.wait([ws_listen_task, stream_listen_task], loop=loop)
    try:
        await call.result
    except prpc.RpcError:
        # TODO: Write to log, but ignore it.
        pass
    return websocket


async def _proxy_websocket_listen_ws(websocket, event_queue):
    """Forward messages from websocket (from client) to event queue.

    Each message is stored as tuple (CLIENT_TO_JOB, payload).
    When websocket is closed for any reason, one last message with 'None'
    payload is emitted.
    """
    async for msg in websocket:
        if msg.type not in (aiohttp.WSMsgType.BINARY, aiohttp.WSMsgType.TEXT):
            break
        await event_queue.put((WSMessageDirection.CLIENT_TO_JOB, msg.data))
    await event_queue.put((WSMessageDirection.CLIENT_TO_JOB, None))


async def _proxy_websocket_listen_stream(stream, event_queue):
    """Forward messages from websocket (from agent job) to event queue.

    Each message is stored as tuple (JOB_TO_CLIENT, payload).
    When websocket is closed for any reason, one last message with 'None'
    payload is emitted.
    """
    async for msg in stream:
        await event_queue.put((WSMessageDirection.JOB_TO_CLIENT, msg))
    await event_queue.put((WSMessageDirection.JOB_TO_CLIENT, None))


async def _proxy_error_response(call):
    """Tranfrom proxy errors into error pages.

    No fancy HTML is used, errors are formatted as plain text.
    """
    try:
        try:
            await asyncio.wait_for(call.result, timeout=PROXY_EXCEPTION_TIMEOUT)
        except asyncio.TimeoutError:
            await call.cancel()
        return aiohttp.web.Response(
            status=http.HTTPStatus.BAD_GATEWAY,
            text='Malformed response from agent.'
        )
    except prpc.RpcError as ex:
        buffer = io.StringIO()
        buffer.write('Proxy error:\n')
        buffer.write('-' * 40)
        buffer.write('\nError type: ')
        buffer.write(type(ex).__name__)
        buffer.write('\n\nError message: ')
        buffer.write(ex.cause_message)
        # TODO: Output only in 'debug mode'?
        buffer.write('\n\n')
        buffer.write(ex.remote_traceback)
        buffer.write('\n')
        return aiohttp.web.Response(
            status=http.HTTPStatus.BAD_GATEWAY,
            text=buffer.getvalue()
        )
