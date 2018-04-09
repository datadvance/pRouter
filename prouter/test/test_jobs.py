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
#

import asyncio
import http
import io
import os
import pathlib
import tarfile
import uuid

import aiohttp.web
import pytest
import yarl

import prpc


@pytest.mark.async_test
async def test_http_proxy(event_loop, router_process, http_server_command):
    """Basic sanity test for HTTP proxy - check HTTP content/status forwarding,
    check websocket connection.
    """
    async with router_process() as router:
        job = await router.job_create()
        await router.job_start(job['path'], http_server_command())

        response = await router.job_http(job['path'], 'GET', '/')
        assert 'TestHeader' in response.headers
        assert response.headers['TestHeader'] == 'hello world!'
        assert response.status == http.HTTPStatus.OK
        payload = await response.text()
        assert payload == 'hello world!'

        response = await router.job_http(job['path'], 'GET', '/no_such_path')
        assert response.status == http.HTTPStatus.NOT_FOUND

        TEST_MESSAGE = b'it\'s alive!'
        websocket = await router.job_ws(job['path'], '/ws_echo')
        await websocket.send_bytes(TEST_MESSAGE)
        assert await websocket.receive_bytes() == TEST_MESSAGE
        await websocket.close()

        await router.job_http(job['path'], 'GET', '/shutdown')
        await router.job_wait(job['path'])


@pytest.mark.async_test
async def test_active_ws(event_loop, router_process, http_server_command):
    """Test active ws connection API - ask router to actively establish
    a tunnel between the client and the job.
    """
    TEST_MESSAGE = b'websocket data'
    TEST_WS_PATH = '/ws_endpoint'
    WS_TIMEOUT = 5

    connected = event_loop.create_future()
    echo_received = event_loop.create_future()

    async def ws_handler(request):
        response = aiohttp.web.WebSocketResponse()
        await response.prepare(request)
        connected.set_result(True)
        await response.send_bytes(TEST_MESSAGE)
        echo = await response.receive_bytes()
        assert echo == TEST_MESSAGE
        echo_received.set_result(True)
        await response.close()
        return response

    app = aiohttp.web.Application()
    app.router.add_get(TEST_WS_PATH, ws_handler)
    server = prpc.platform.ws_aiohttp.AsyncServer(
        app, [('127.0.0.1', 0)]
    )

    async with server as endpoints:
        (test_address, test_port), = endpoints
        async with router_process() as router:
            job = await router.job_create()
            await router.job_start(job['path'], http_server_command())

            response = await router.session.request(
                'POST',
                router.endpoint_control.with_path(
                    job['path'] + '/wsconnect' + '/ws_echo'
                ),
                json={
                    'url': yarl.URL.build(
                        scheme='http',
                        host=test_address,
                        port=test_port,
                        path=TEST_WS_PATH
                    ).__str__()
                }
            )
            assert response.status == http.HTTPStatus.OK

            await asyncio.wait_for(connected, WS_TIMEOUT, loop=event_loop)
            await asyncio.wait_for(echo_received, WS_TIMEOUT, loop=event_loop)

            await router.job_http(job['path'], 'GET', '/shutdown')
            await router.job_wait(job['path'])


@pytest.mark.async_test
async def test_upload_download(event_loop, router_process):
    """Check basic file upload/download features."""
    TEST_DATA_PAYLOAD = uuid.uuid4().bytes * (1 << 20)
    UPLOAD_FILENAME = 'the_data.bin'

    async with router_process() as router:
        job = await router.job_create()
        response = await router.session.request(
            'POST',
            router.endpoint_control.with_path(
                job['path'] + '/file/' + UPLOAD_FILENAME
            ),
            headers={
                'Content-Type': 'application/octet-stream',
                'Content-Length': str(len(TEST_DATA_PAYLOAD))
            },
            data=io.BytesIO(TEST_DATA_PAYLOAD)
        )
        async with response:
            assert response.status == http.HTTPStatus.OK
        response = await router.session.request(
            'GET',
            router.endpoint_control.with_path(
                job['path'] + '/file/' + UPLOAD_FILENAME
            )
        )
        async with response:
            assert response.status == http.HTTPStatus.OK
            buffer = io.BytesIO()
            chunk = await response.content.readany()
            while chunk:
                buffer.write(chunk)
                chunk = await response.content.readany()
            data = buffer.getvalue()
            assert data == TEST_DATA_PAYLOAD
        await router.job_remove(job['path'])


@pytest.mark.async_test
async def test_upload_download_archive(event_loop, router_process, tmpdir):
    """Check archive upload/download features.

    More checks (unicode etc) are in corresponding test in pAgent.
    """
    DATA_PATH = pathlib.Path(tmpdir.mkdir('data'))
    UPLOAD_PATH = pathlib.Path(tmpdir.join('upload.tar'))
    DOWNLOAD_ARC = pathlib.Path(tmpdir.join('download.tar'))
    DOWNLOAD_PATH = pathlib.Path(tmpdir.join('download'))
    FILES = [
        'something.py',
        'whatever.txt',
        'nested/something_too.py'
    ]
    for filename in FILES:
        target = DATA_PATH.joinpath(filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, 'w') as target_file:
            target_file.write(uuid.uuid4().hex * 128)
    with tarfile.open(UPLOAD_PATH, 'w') as arc:
        for file_path in DATA_PATH.iterdir():
            arc.add(file_path, arcname=file_path.name)
    async with router_process() as router:
        job = await router.job_create()
        with open(UPLOAD_PATH, 'rb') as arc:
            response = await router.session.request(
                'POST',
                router.endpoint_control.with_path(
                    job['path'] + '/archive'
                ),
                headers={
                    'Content-Type': 'application/octet-stream',
                    'Content-Length': str(UPLOAD_PATH.stat().st_size)
                },
                data=arc
            )
            async with response:
                assert response.status == http.HTTPStatus.OK
        with open(DOWNLOAD_ARC, 'wb') as arc:
            response = await router.session.request(
                'GET',
                router.endpoint_control.with_path(
                    job['path'] + '/archive'
                ).with_query(
                    {
                        'include': '*.py',
                        'exclude': 'nested/*',
                        'compress': '0'
                    }
                )
            )
            async with response:
                assert response.status == http.HTTPStatus.OK
                chunk = await response.content.readany()
                while chunk:
                    arc.write(chunk)
                    chunk = await response.content.readany()
        with tarfile.open(DOWNLOAD_ARC, 'r') as arc:
            arc.extractall(DOWNLOAD_PATH)
        assert len(os.listdir(DOWNLOAD_PATH)) == 1
        with open(DATA_PATH.joinpath('something.py')) as uploaded:
            with open(DOWNLOAD_PATH.joinpath('something.py')) as downloaded:
                assert uploaded.read() == downloaded.read()


@pytest.mark.async_test
async def test_job_not_found(event_loop, router_process):
    """Check HTTP responses if job or connection do not exist."""
    async with router_process() as router:
        job = await router.job_create()
        await router.job_info(job['path'])
        # Check wrong connection id.
        response = await router.session.request(
            'GET',
            router.endpoint_control.with_path(
                '/jobs/' + 'wrong_connection_id/' + job['uid'] + '/info'
            )
        )
        async with response:
            message = await response.text()
            assert 'connection' in message
            assert 'not found' in message
            assert response.status == http.HTTPStatus.NOT_FOUND

        # Check wrong job id.
        response = await router.session.request(
            'GET',
            router.endpoint_control.with_path(
                '/'.join(job['path'].split('/')[:3]) + '/wrong_job_id' + '/info'
            )
        )
        async with response:
            message = await response.text()
            assert 'job' in message
            assert 'not found' in message
            assert response.status == http.HTTPStatus.NOT_FOUND
        await router.job_remove(job['path'])


@pytest.mark.async_test
async def test_invalid_request(event_loop, router_process):
    """Check HTTP response on invalid job_create request payload."""
    async with router_process() as router:
        # Don't try to exhaustively test all commands, just check the
        # general response for jsonschema errors (they should lead to
        # status 400 instead of 500).
        response = await router.session.request(
            'POST',
            router.endpoint_control.with_path(
                '/jobs/create'
            ),
            json={
                'name': 'invalid job',
                'agent': {
                    'wrong key here': 'and some whatever data'
                }
            }
        )
        async with response:
            message = await response.text()
            assert 'Invalid request' in message
            assert response.status == http.HTTPStatus.BAD_REQUEST
