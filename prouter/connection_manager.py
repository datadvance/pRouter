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

import logging

import prpc

from . import identity


class ConnectionNotFound(Exception):
    """Cannot find requested connection instance."""


class ConnectionManager(object):
    """Connection registry.

    Roles:
      * Register/unregister each connection
      * List all connections (for debug/UI)
      * Close all connections (on application exit)
    """

    AGENT_RPC_PATH = '/rpc/v1'
    DEFAULT_LOG_NAME = 'prouter.ConnectionManager'

    def __init__(self, debug=False, polling_delay=5, logger=None):
        self._log = logger or logging.getLogger(self.DEFAULT_LOG_NAME)
        self._debug = debug
        self._polling_delay = polling_delay
        self._connections = {}
        self._incoming_by_uid = {}

    @property
    def debug(self):
        """Config parameter enabling debug mode for all connections."""
        return self._debug

    @property
    def polling_delay(self):
        """Config parameter controlling poll rate of active connections."""
        return self._polling_delay

    def get_connections(self):
        """Get all active connections."""
        return list(self._connections.values())

    def connection(self, connection_uid):
        """Get connection by connection uid.

        Note: connection uid is unrelated to the peer uid.
        """
        try:
            return self._connections[connection_uid]
        except KeyError:
            raise ConnectionNotFound(
                'connection \'%s\' is not found' % (connection_uid,)
            )

    def by_peer_uid(self, agent_uid):
        """Get connection by peer uid."""
        connection = self._incoming_by_uid.get(agent_uid)
        if connection is None:
            raise ConnectionNotFound(
                'no connected agent with uid \'%s\'' % (agent_uid,)
            )
        return connection

    def register(self, connection, handshake):
        """Try to register new connection with given handshake."""
        assert connection.mode in prpc.ConnectionMode
        assert connection.mode != prpc.ConnectionMode.NEW
        # In fact, we should wait for key for some time
        # before raising.
        #
        # However, proper implementation (condition etc)
        # is unfeasibly complicated for now and polling
        # is too ugly.
        peer_uid = identity.Identity.get_uid(handshake)
        if connection.id in self._connections:
            raise ValueError(
                'connection \'%s\' (mode: %s) is already registered' %
                (connection.id, connection.mode)
            )
        if connection.mode == prpc.ConnectionMode.SERVER:
            if peer_uid in self._incoming_by_uid:
                raise ValueError(
                    'incoming connection from peer \'%s\' '
                    'is already registered' % (peer_uid,)
                )
            self._incoming_by_uid[peer_uid] = connection
        connection.on_close.append(self._unregister)
        self._connections[connection.id] = connection
        self._log.info(
            'New connection: id \'%s\', mode: %s, peer: \'%s\', token: \'%s\'',
            connection.id,
            connection.mode.name,
            peer_uid,
            identity.Identity.get_token(handshake)
        )

    def _unregister(self, connection):
        """Unregisters connection when it is closed."""
        del self._connections[connection.id]
        peer_uid = identity.Identity.get_uid(connection.handshake_data)
        if connection.mode == prpc.ConnectionMode.SERVER:
            del self._incoming_by_uid[peer_uid]
        self._log.info(
            'Dropped connection: '
            'id \'%s\', mode: %s, peer: \'%s\', token: \'%s\'',
            connection.id,
            connection.mode.name,
            identity.Identity.get_uid(connection.handshake_data),
            identity.Identity.get_token(connection.handshake_data)
        )
        connection.on_close.remove(self._unregister)
