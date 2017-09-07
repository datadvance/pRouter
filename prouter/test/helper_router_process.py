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
#

import asyncio
import http
import logging
import pathlib
import sys
import uuid

import aiohttp
import prpc
import yarl

from prouter import control_app
from prouter import router_app
from pagent import polled_process
from pagent.test import helper_agent_process


LOCALHOST_WS_URL = yarl.URL("ws://127.0.0.1")


class RouterProcess(object):
    def __init__(self, *args, client_enabled=True, loop=None):
        self._log = logging.getLogger('RouterProcess')
        self._loop = loop
        self._client_enabled = client_enabled
        self._accepted_token = uuid.uuid4().hex
        self._agent_uid = uuid.uuid4().hex
        process_args = [sys.executable, '-m', 'prouter']
        process_args.extend([
            '--connection-debug'
        ])
        process_args.extend([
            '--set',
            'server.accept_tokens=["%s"]' % (self._accepted_token,)
        ])
        process_args.extend(args)
        self._process = polled_process.PolledProcess(
            process_args,
            None,
            loop=self._loop
        )
        self._session = None
        self._port_control = None
        self._port_router = None
        self._agent_process = None

    @property
    def process(self):
        return self._process

    @property
    def endpoint_control(self):
        return LOCALHOST_WS_URL.with_port(self._port_control)

    @property
    def endpoint_router(self):
        return LOCALHOST_WS_URL.with_port(self._port_router).with_path(
            router_app.ROUTE_RPC_SERVER
        )

    @property
    def session(self):
        return self._session

    @property
    def accepted_token(self):
        return self._accepted_token

    @property
    def agent_uid(self):
        return self._agent_uid

    @property
    def agent(self):
        return self._agent_process

    async def job_create(self, by_uid=True):
        if by_uid:
            agent_id = {
                'type': 'uid',
                'uid': self._agent_uid
            }
        else:
            address = "%s:%d" % (
                self._agent_process.endpoint_agent.host,
                self._agent_process.endpoint_agent.port
            )
            agent_id = {
                'type': 'address',
                'address': address,
                'token': self._agent_process.accepted_token
            }
        response = await self._session.request(
            'POST',
            self.endpoint_control.with_path(control_app.ROUTE_JOB_CREATE),
            json={
                'name': 'test job',
                'agent': agent_id
            }
        )
        assert response.status == http.HTTPStatus.OK
        return await response.json()

    async def job_remove(self, job_path):
        async with self._session.request(
            'POST',
            self.endpoint_control.with_path(job_path + '/remove')
        ) as response:
            assert response.status == http.HTTPStatus.OK
            return await response.json()

    async def job_wait(self, job_path):
        async with self._session.request(
            'POST',
            self.endpoint_control.with_path(job_path + '/wait')
        ) as response:
            assert response.status == http.HTTPStatus.OK
            return await response.json()

    async def job_info(self, job_path):
        async with self._session.request(
            'GET',
            self.endpoint_control.with_path(job_path + '/info')
        ) as response:
            assert response.status == http.HTTPStatus.OK
            return await response.json()

    async def job_start(self, job_path, args, env={}, expected_port_count=1):
        async with self._session.request(
            'POST',
            self.endpoint_control.with_path(job_path + '/start'),
            json={
                'args': args,
                'env': env,
                'expected_port_count': expected_port_count
            }
        ) as response:
            assert response.status == http.HTTPStatus.OK
            return await response.json()

    async def job_http(self, job_path, method, path, **kwargs):
        response = await self._session.request(
            method,
            self.endpoint_control.with_path(job_path + '/http' + path),
            **kwargs
        )
        return response

    async def job_ws(self, job_path, path, **kwargs):
        response = await self._session.ws_connect(
            self.endpoint_control.with_path(job_path + '/http' + path),
            **kwargs
        )
        return response


    async def connections(self):
        async with self._session.request(
            'GET',
            self.endpoint_control.with_path(control_app.ROUTE_CONNECTIONS)
        ) as response:
            assert response.status == http.HTTPStatus.OK
            return await response.json()

    async def __aenter__(self):
        CONNECT_TIMEOUT = 10
        CONNECT_POLL_DELAY = 0.1
        AGENT_RECONNECT_DELAY = 1
        self._log.info('Starting router process')
        await self._process.start(
            workdir=pathlib.Path(__file__).absolute().parents[2],
            port_expected_count=2
        )
        self._session = aiohttp.ClientSession(loop=self._loop)
        try:
            assert len(self._process.ports) == 2
            self._log.info('Startup successfull, detecting control port')
            ports = list(self._process.ports)
            for port in self._process.ports:
                response = await self._session.get(
                    LOCALHOST_WS_URL.with_port(port).with_path(
                        control_app.ROUTE_INFO
                    )
                )
                if response.status != http.HTTPStatus.NOT_FOUND:
                    self._port_control = port
                    break
            assert self._port_control is not None
            ports.remove(self._port_control)
            self._port_router, = ports
            self._log.info('Control port: %d', self._port_control)
            self._log.info('Router port:  %d', self._port_router)

            # Start the agent.
            self._agent_process = helper_agent_process.AgentProcess(
                '--set', 'identity.uid="%s"' % (self._agent_uid,),
                '--set', 'client.enabled=%s' % (self._client_enabled,),
                '--set', 'client.address="127.0.0.1:%d"' % (
                    self._port_router,
                ),
                '--set', 'client.token="%s"' % (self._accepted_token,),
                '--set', 'client.reconnect_delay=%d' % (AGENT_RECONNECT_DELAY,),
                # We don't need direct RPC connection to the agent.
                connect=False,
                loop=self._loop
            )
            await self._agent_process.__aenter__()
            if self._client_enabled:
                # Wait until agent connects to router.
                connected = False
                agent_connect_start = self._loop.time()
                while self._loop.time() - agent_connect_start < CONNECT_TIMEOUT:
                    response = await self._session.get(
                        self.endpoint_control.with_path(
                            control_app.ROUTE_CONNECTIONS
                        )
                    )
                    connections = await response.json()
                    target_connections = [
                        conn for conn in connections['connections']
                        if conn['peer']['auth']['uid'] == self._agent_uid
                    ]
                    if len(target_connections) == 1:
                        connected = True
                        self._log.info('Agent successfully connected to router')
                        break
                    await asyncio.sleep(CONNECT_POLL_DELAY, loop=self._loop)
                if not connected:
                    raise RuntimeError(
                        'agent failed to connect to router in time'
                    )
        except Exception:
            await self._process.kill()
            if self._agent_process and self._agent_process.running:
                await self._agent_process.__aexit__()
            raise
        return self

    async def __aexit__(self, *ex_args):
        KILL_DELAY = 5.
        # Shutdown the agent if it's running.
        if self._agent_process is not None:
            await self._agent_process.__aexit__()
        # Shutdown the router.
        if self._process.state == polled_process.ProcessState.RUNNING:
            self._log.debug('Sending shutdown command using control API')
            await self._session.post(
                self.endpoint_control.with_path(control_app.ROUTE_SHUTDOWN)
            )
            try:
                await asyncio.wait_for(
                    self._process.wait(), KILL_DELAY, loop=self._loop
                )
                self._log.debug('Router finalized successfully')
            except asyncio.TimeoutError:
                await self._process.kill()
        self._agent_process = None
        self._port_control = None
        self._port_router = None
        self._session.close()
        return False
