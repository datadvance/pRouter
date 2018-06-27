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

import aiohttp.web

import pagent.handlers.signals

from . import handlers


_ROUTE_PREFIX_JOBS = '/jobs/{%s}/{%s}' % (
    handlers.jobs.ROUTE_VARIABLE_CONNECTION_UID,
    handlers.jobs.ROUTE_VARIABLE_JOB_UID
)


ROUTE_INFO = '/info'
ROUTE_CONNECTIONS = '/connections'
ROUTE_SHUTDOWN = '/shutdown'
ROUTE_JOB_CREATE = '/jobs/create'
ROUTE_JOB_REMOVE = _ROUTE_PREFIX_JOBS + '/remove'
ROUTE_JOB_WAIT = _ROUTE_PREFIX_JOBS + '/wait'
ROUTE_JOB_INFO = _ROUTE_PREFIX_JOBS + '/info'
ROUTE_JOB_START = _ROUTE_PREFIX_JOBS + '/start'
ROUTE_JOB_HTTP = _ROUTE_PREFIX_JOBS + '/http/{%s:.*}' % (
    handlers.proxy.ROUTE_VARIABLE_PATH,
)
ROUTE_JOB_WS_ACTIVE = _ROUTE_PREFIX_JOBS + '/wsconnect/{%s:.*}' % (
    handlers.proxy.ROUTE_VARIABLE_PATH,
)
ROUTE_JOB_FILE_API = _ROUTE_PREFIX_JOBS + '/file/{%s:.+}' % (
    handlers.files.ROUTE_VARIABLE_FSPATH,
)
ROUTE_JOB_ARCHIVE_API = _ROUTE_PREFIX_JOBS + '/archive'


ROUTES = [
    ('GET', ROUTE_INFO, handlers.admin.info),
    ('GET', ROUTE_CONNECTIONS, handlers.admin.connections),
    ('POST', ROUTE_SHUTDOWN, handlers.admin.shutdown),
    ('POST', ROUTE_JOB_CREATE, handlers.jobs.job_create),
    ('POST', ROUTE_JOB_REMOVE, handlers.jobs.job_remove),
    ('POST', ROUTE_JOB_WAIT, handlers.jobs.job_wait),
    ('GET', ROUTE_JOB_INFO, handlers.jobs.job_info),
    ('POST', ROUTE_JOB_START, handlers.jobs.job_start),
    ('*', ROUTE_JOB_HTTP, handlers.proxy.proxy_passive),
    ('POST', ROUTE_JOB_WS_ACTIVE, handlers.proxy.proxy_active),
    ('*', ROUTE_JOB_FILE_API, handlers.files.single_file),
    ('*', ROUTE_JOB_ARCHIVE_API, handlers.files.archive)
]


MIDDLEWARES = [
    handlers.middleware.error_middleware,
    aiohttp.web.normalize_path_middleware(append_slash=False)
]


def get_application(connection_manager, identity, exit_handler, logger):
    """Creates the control web application.

    Control app exposes 3 main APIs:
        * Create/run/remove jobs.
        * Proxy HTTP/WS request to jobs.
        * General router admin (list active connections etc).
    """
    app = aiohttp.web.Application(logger=logger, middlewares=MIDDLEWARES)
    app[handlers.common.KEY_CONN_MANAGER] = connection_manager
    app[handlers.common.KEY_IDENTITY] = identity
    app[handlers.admin.KEY_EXIT_HANDLER] = exit_handler

    app.on_response_prepare.append(pagent.handlers.signals.disable_cache)

    for method, route, handler in ROUTES:
        app.router.add_route(method, route, handler)

    return app
