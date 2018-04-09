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

from . import common


KEY_EXIT_HANDLER = 'exit_handler'


async def info(request):
    """Return info about router instance."""
    identity = request.app[common.KEY_IDENTITY]
    return aiohttp.web.json_response(data=identity.get_server_handshake())


async def connections(request):
    """Return info about open connections."""
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    result = []
    for connection in conn_manager.get_connections():
        connection_desc = {
            'uid': connection.id,
            'mode': connection.mode.name,
            'peer': connection.handshake_data
        }
        result.append(connection_desc)
    return aiohttp.web.json_response(data={'connections': result})


async def shutdown(request):
    """Handle an incoming shutdown request."""
    request.app[KEY_EXIT_HANDLER]()
    # Return valid empty response.
    return aiohttp.web.Response()
