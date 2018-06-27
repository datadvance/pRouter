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

"""Router client."""


import aiohttp.web
import yarl

from .job_client import JobClient


class RouterClient(object):
    """Router client.

    Intended to simplify interaction with a router. Communicates with
    it through the router HTTP interface.

    Args:
        address: Router address.
        session: AIOHTTP client session.
        loop: Asyncio eventloop.
    """

    def __init__(self, address, session=None, loop=None):
        self._url = yarl.URL(address)
        if not self._url.is_absolute():
            self._url = yarl.URL('http://%s' % (address))
        self._session = session or aiohttp.ClientSession(loop=loop)

    @property
    def url(self):
        """Router url."""
        return self._url

    @property
    def session(self):
        """Client session."""
        return self._session

    async def connections(self):
        """Information about router connections as json."""
        return await self.request('GET', '/connections')

    async def request(self, method, path, result=True, kwds=None):
        """Make HTTP request to the router. """
        response = await self._session.request(
            method, self._url.with_path(path), **(kwds or {})
        )
        async with response:
            response.raise_for_status()
            if result:
                return await response.json()

    async def create_job(self, name, agent=None):
        """Create a new router job.
        Args:
            name: The job name.
            agent: Agent to run job, agent type selected by value type:
                string: agent uid
                sequence of strings: agent uids, router will select one
                dict: dictionary with two fields:
                    'address': agent address
                    'token: agent authorization token
        Return: JobClient instance."""

        agent_locator = {}
        if isinstance(agent, str):
            agent_locator['type'] = 'uid'
            agent_locator['uid'] = agent
        elif isinstance(agent, dict):
            agent_locator['type'] = 'address'
            agent_locator['address'] = agent['address']
            agent_locator['token'] = agent['token']
        else:
            agent_locator['type'] = 'select'
            agent_locator['uids'] = list(agent)

        return await JobClient.create(
            self,
            arguments={'json': {'name': name, 'agent': agent_locator}}
        )

    async def attach_job(self, job):
        """Attach to an existing job by its path or info.

        Args:
            job: Job path (string) or job info (dict).
        """
        assert isinstance(job, (str, dict)), ('Argument `job` is neither job '
                                              'path nor job info!')
        job_path = job['path'] if isinstance(job, dict) else job

        return await JobClient.attach(self, job_path)
