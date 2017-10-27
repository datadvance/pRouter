#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
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

import aiohttp.web
import pagent.agent_service

from . import common
from . import jobs


ROUTE_VARIABLE_FSPATH = 'fspath'

QUERY_KEY_REMOVE_FILE = 'remove'
QUERY_KEY_EXECUTABLE = 'executable'
QUERY_KEY_INCLUDE = 'include'
QUERY_KEY_EXCLUDE = 'exclude'
QUERY_KEY_COMPRESS = 'compress'

CONTENT_TYPE_BINARY = 'application/octet-stream'


async def single_file(request):
    """Handle file upload/download requests.

    GET is download, POST is upload.
    """
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    fspath = request.match_info[ROUTE_VARIABLE_FSPATH]
    connection = conn_manager.connection(conn_uid)
    if request.method == 'GET':
        # Optional query param: remove=0/1
        remove_source = bool(
            int(request.query.getone(QUERY_KEY_REMOVE_FILE, 0))
        )
        rpc_call = connection.call_istream(
            pagent.agent_service.AgentService.file_download.__name__,
            [job_uid, fspath],
            {
                'remove': remove_source
            }
        )
        return await _send_file(request, rpc_call)
    elif request.method == 'POST':
        # Optional query param: executable=0/1
        executable_flag = bool(
            int(request.query.getone(QUERY_KEY_EXECUTABLE, 0))
        )
        if request.content_type == CONTENT_TYPE_BINARY:
            if request.content_length is None:
                raise common.InvalidRequestData('no Content-Length provided')

            rpc_call = connection.call_ostream(
                pagent.agent_service.AgentService.file_upload.__name__,
                [job_uid, fspath],
                {
                    'executable': executable_flag
                }
            )
            await _accept_file(request, rpc_call)
        # TODO: Support form data.
        else:
            raise common.InvalidRequestData(
                'unsupported content type for HTTP upload'
            )

        return aiohttp.web.Response()
    else:
        raise common.InvalidRequestData('unsupported HTTP method')


async def archive(request):
    """Handle batch (.tar archive) upload/download requests.

    GET is download, POST is upload.
    """
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    connection = conn_manager.connection(conn_uid)
    if request.method == 'GET':
        # Optional query param: include=<mask>
        include_mask = request.query.getone(QUERY_KEY_INCLUDE, None)
        # Optional query param: exclude=<mask>
        exclude_mask = request.query.getone(QUERY_KEY_EXCLUDE, None)
        # Optional query param: compress=0/1
        compress = bool(
            int(request.query.getone(QUERY_KEY_COMPRESS, False))
        )
        rpc_call = connection.call_istream(
            pagent.agent_service.AgentService.archive_download.__name__,
            [job_uid],
            {
                'include_mask': include_mask,
                'exclude_mask': exclude_mask,
                'compress': compress
            }
        )
        return await _send_file(request, rpc_call)
    elif request.method == 'POST':
        if request.content_type == CONTENT_TYPE_BINARY:
            if request.content_length is None:
                raise common.InvalidRequestData('no Content-Length provided')

            rpc_call = connection.call_ostream(
                pagent.agent_service.AgentService.archive_upload.__name__,
                [job_uid]
            )
            await _accept_file(request, rpc_call)
        # TODO: Support form data.
        else:
            raise common.InvalidRequestData(
                'unsupported content type for HTTP upload'
            )

        return aiohttp.web.Response()
    else:
        raise common.InvalidRequestData('unsupported HTTP method')


async def _send_file(request, rpc_call):
    response = aiohttp.web.StreamResponse()
    async with rpc_call:
        header = await rpc_call.stream.receive()
        # Download started successfully.
        if header is not None:
            # We cannot change response status if anything goes wrong
            # while download in progress, but client can detect
            # failed download by Content-Length.
            response.content_type = CONTENT_TYPE_BINARY
            response.content_length = header['size']
            await response.prepare(request)
            async for chunk in rpc_call.stream:
                response.write(chunk)
                await response.drain()
        # Failed immediately.
        # TODO: Specialized error handling, esp. file does not exist?
        else:
            await rpc_call.result
    return response


async def _accept_file(request, rpc_call):
    received_size = 0
    async with rpc_call:
        chunk = await request.content.readany()
        while chunk:
            received_size += len(chunk)
            if received_size > request.content_length:
                raise common.InvalidRequestData(
                    'request payload size does not '
                    'match passed Content-Length'
                )
            await rpc_call.stream.send(chunk)
            chunk = await request.content.readany()
        accepted_size = await rpc_call.result
        if accepted_size != received_size:
            raise ValueError('upload failed')
