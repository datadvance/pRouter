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

CONTENT_TYPE_BINARY = 'application/octet-stream'


async def download_or_upload(request):
    """Demultiplex the file API request by the HTTP method.

    GET is download, POST is upload.
    """
    if request.method == 'GET':
        return await download(request)
    elif request.method == 'POST':
        return await upload(request)
    else:
        raise common.InvalidRequestData('unsupported HTTP method')

async def download(request):
    """Download a file from the job sandbox.
    """
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    fspath = request.match_info[ROUTE_VARIABLE_FSPATH]
    connection = conn_manager.connection(conn_uid)
    rpc_call = connection.call_istream(
        pagent.agent_service.AgentService.file_download.__name__,
        [job_uid, fspath]
    )
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


async def upload(request):
    """Upload a file from to the job sandbox.
    """
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[jobs.ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[jobs.ROUTE_VARIABLE_JOB_UID]
    fspath = request.match_info[ROUTE_VARIABLE_FSPATH]
    connection = conn_manager.connection(conn_uid)

    if request.content_type == CONTENT_TYPE_BINARY:
        if request.content_length is None:
            raise common.InvalidRequestData(
                'no Content-Length provided'
            )

        rpc_call = connection.call_ostream(
            pagent.agent_service.AgentService.file_upload.__name__,
            [job_uid, fspath]
        )

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
    # TODO: Support form data.
    else:
        raise common.InvalidRequestData(
            'unsupported content type for HTTP upload'
        )

    return aiohttp.web.Response()
