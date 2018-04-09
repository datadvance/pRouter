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

import http

import aiohttp.web
import jsonschema

import prpc

from . import common
from .. import connection_manager


RPC_ERROR_JOB_NOT_FOUND = 'JobNotFoundError'


async def error_middleware(app, handler):
    """Custom error handling middleware factory.

    Transforms common exceptions to helpful HTTP statuses instead
    of default 500/InternalServerError.
    """
    async def error_handler(request):
        """Error middleware implementation."""
        try:
            return await handler(request)
        except common.InvalidRequestData as ex:
            return aiohttp.web.Response(
                text=str(ex), status=http.HTTPStatus.BAD_REQUEST
            )
        except jsonschema.ValidationError as ex:
            return aiohttp.web.Response(
                text=('Invalid request payload:\n' + str(ex)),
                status=http.HTTPStatus.BAD_REQUEST
            )
        except connection_manager.ConnectionNotFound as ex:
            return aiohttp.web.Response(
                text=str(ex), status=http.HTTPStatus.NOT_FOUND
            )
        except prpc.RpcMethodError as ex:
            if ex.cause_type == RPC_ERROR_JOB_NOT_FOUND:
                return aiohttp.web.Response(
                    text=ex.cause_message, status=http.HTTPStatus.NOT_FOUND
                )
            else:
                raise
    return error_handler
