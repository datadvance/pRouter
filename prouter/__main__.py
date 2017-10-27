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

import argparse
import asyncio
import enum
import http
import io
import logging
import pathlib
import signal
import sys
import traceback

import aiohttp
import aiohttp.web
import prpc
import yarl

import prouter.config
import prouter.connection_manager
import prouter.control_app
import prouter.identity
import prouter.router_app


LOGGER_NAME_ROOT = 'prouter'
LOGGER_NAME_CONTROL_APP = '.'.join((LOGGER_NAME_ROOT, 'ControlApplication'))
LOGGER_NAME_CONTROL_SERVER = '.'.join((LOGGER_NAME_ROOT, 'ControlServer'))
LOGGER_NAME_ROUTER_APP = '.'.join((LOGGER_NAME_ROOT, 'AgentApplication'))
LOGGER_NAME_ROUTER_SERVER = '.'.join((LOGGER_NAME_ROOT, 'AgentServer'))


def setup_logging(args):
    root_logger = logging.getLogger()
    # root logger accepts all messages, filter on handlers level
    root_logger.setLevel(logging.DEBUG)
    for handler in [logging.StreamHandler(sys.stdout)]:
        handler.setFormatter(logging.Formatter(args.log_format))
        handler.setLevel(args.log_level.upper())
        root_logger.addHandler(handler)


class ExitHandler(object):
    """Manage exit process - close all long-running tasks.

    Grouped into a class to avoid using globals
    (self._exit_task would be global so main can wait for it).
    """
    def __init__(self, servers, conn_manager, logger, loop):
        self._servers = servers
        self._conn_manager = conn_manager
        self._log = logger
        self._loop = loop
        self._exit_task = None
        self._exit_sent = False
        self._exit_code = 0

    def __call__(self, exit_code=0):
        """Submit exit task to the event loop. Ignores multiple calls."""
        if self._exit_sent:
            self._log.debug('Exit in progress, exit request ignored')
            return
        self._log.info('Exit request received')
        self._exit_task = self._loop.create_task(self._loop_exit())
        self._exit_sent = True
        self._exit_code = exit_code

    async def wait(self):
        """If exit task is active, wait for it to finish."""
        if self._exit_task:
            await self._exit_task
        return self._exit_code

    async def _loop_exit(self):
        """Actual exit/cleanup implementation."""
        # Cleanup current connections.
        # It's done before disabling the server, so that
        # asyncio is less unhappy.
        for connection in self._conn_manager.get_connections():
            await connection.close()
        # Shutdown the server(s) so we don't accept new ones.
        for server in reversed(self._servers):
            await server.shutdown()
        # Cleanup any leftovers.
        for connection in self._conn_manager.get_connections():
            await connection.close()


def make_signal_handler(exit_handler, loop):
    def on_exit(signo, frame):
        # Surprisingly enough, threadsafe loop API is not only 'safe'
        # but also wakes up the loop immediately (so you don't wait
        # for timeout on select/epoll/whatever).
        #
        # Note:
        #   As about threadsafety, it shouldn't be important.
        #   Python docs state that python signal handlers are called
        #   in the main thread anyway (even if e.g. Windows calls
        #   native handlers in a separate thread).
        loop.call_soon_threadsafe(exit_handler)
    return on_exit


async def run(args, config, main_log, loop):
    servers = []
    lifetime_tasks = []

    identity = prouter.identity.Identity(
        config['identity']['uid'],
        config['identity']['name'],
        config['server']['accept_tokens']
    )

    conn_manager = prouter.connection_manager.ConnectionManager(
        debug=args.connection_debug,
        polling_delay=config['client']['polling_delay']
    )

    # Note the nice circular dependency - control app needs the
    # exit handler, which will actually shutdown the control app.
    #
    # All hail the mutable 'servers' collection
    # that allows us to unroll this.
    exit_handler = ExitHandler(servers, conn_manager, main_log, loop)

    router_app = prouter.router_app.get_application(
        conn_manager, identity, logging.getLogger(LOGGER_NAME_ROUTER_APP)
    )
    control_app = prouter.control_app.get_application(
        conn_manager, identity, exit_handler,
        logging.getLogger(LOGGER_NAME_CONTROL_APP)
    )

    if config['server']['enabled']:
        main_log.info('Server mode enabled')
        agent_server = prpc.platform.ws_aiohttp.AsyncServer(
            router_app,
            endpoints=(
                (config['server']['interface'], config['server']['port']),
            ),
            logger=logging.getLogger(LOGGER_NAME_ROUTER_SERVER)
        )
        await agent_server.start()
        servers.append(agent_server)
        lifetime_tasks.append(agent_server.wait())

    control_server = prpc.platform.ws_aiohttp.AsyncServer(
        control_app,
        endpoints=(
            (config['control']['interface'], config['control']['port']),
        ),
        logger=logging.getLogger(LOGGER_NAME_CONTROL_SERVER)
    )
    await control_server.start()
    servers.append(control_server)
    lifetime_tasks.append(control_server.wait())

    signal.signal(signal.SIGINT, make_signal_handler(exit_handler, loop))
    await asyncio.wait(lifetime_tasks, loop=loop)


def main():
    args, config = prouter.config.initialize()
    setup_logging(args)
    prouter.config.validate(config)

    main_log = logging.getLogger(LOGGER_NAME_ROOT)
    main_log.info('Staring pRouter')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(args, config, main_log, loop))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == '__main__':
    main()
