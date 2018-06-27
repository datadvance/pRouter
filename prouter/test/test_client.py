#
# coding: utf-8
# Copyright (c) 2018 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import asyncio

import pytest


@pytest.mark.async_test
async def test_http_job(event_loop, router_process, http_server_command):
    """Basic sanity test - create/run a simple job, run a HTTP request to it."""
    async with router_process(client_enabled=False) as router:
        job = await router.job_create(by_uid=False)
        await router.job_start(job['path'], http_server_command())
        response = await router.job_http(job['path'], 'GET', '/')
        payload = await response.text()
        assert payload == 'hello world!'
        await router.job_http(job['path'], 'GET', '/shutdown')
        await router.job_wait(job['path'])
        await router.job_remove(job['path'])


@pytest.mark.async_test
async def test_connection_timeout(event_loop, router_process,
                                  http_server_command):
    """Check that router properly drops outgoing connection that no longer
    has an associated job.
    """
    CONNECTION_TIMEOUT_MAX_WAIT = 10
    CONNECTION_CHECK_DELAY = 0.1
    async with router_process(
        '--set',
        'client.polling_delay=%f' % (CONNECTION_CHECK_DELAY,),
        client_enabled=False
    ) as router:
        job = await router.job_create(by_uid=False)
        await router.job_start(job['path'], http_server_command())
        connection_list = (await router.connections())['connections']
        assert len(connection_list) == 1
        await router.job_http(job['path'], 'GET', '/shutdown')
        await router.job_wait(job['path'])
        await router.job_remove(job['path'])
        # Job is removed, so connection should be dropped.
        start = event_loop.time()
        while True:
            connection_list = (await router.connections())['connections']
            if not connection_list:
                break
            if event_loop.time() - start > CONNECTION_TIMEOUT_MAX_WAIT:
                raise RuntimeError('connection is not dropped for too long')
            await asyncio.sleep(CONNECTION_CHECK_DELAY, loop=event_loop)
