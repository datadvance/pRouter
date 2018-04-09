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

import aiohttp.web
import yarl

from prouter import control_app

from .job import Job


class Router(object):
    """ Remote access to router.
    Args:
        address: router address.
        session: aiohttp client session.
        loop: asyncio eventloop.
    """
    def __init__(self, address, session=None, loop=None):
        self._url = yarl.URL(address)
        if not self._url.is_absolute():
            self._url = yarl.URL('http://%s' % (address))
        self._session = session or aiohttp.ClientSession(loop=loop)

    @property
    def url(self):
        """ Returns: router url. """
        return self._url

    @property
    def session(self):
        """ Returns: client session. """
        return self._session

    async def connections(self):
        """ Returns: information about router connections as json. """
        return await self.request('GET', control_app.ROUTE_CONNECTIONS)

    async def request(self, method, path, result=True, kwargs=None):
        """ Send http request to router. """
        response = await self._session.request(
            method, self._url.with_path(path), **(kwargs or {})
        )
        async with response:
            response.raise_for_status()
            if result:
                return await response.json()

    def job(self, name, uid=None, address=None, token=None, runtimes=None):
        """ Create job on specified agent.
        Args:
            name: job name.
            uid: agent uid, either uid or address and token must be specified.
            address: agent address.
            token: agent access token.
        Returns: new job instance.
        """
        return Job(self, name, uid, address, token, runtimes)
