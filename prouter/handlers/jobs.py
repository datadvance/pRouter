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

import aiohttp.web
import jsonschema
import yarl

import pagent.agent_service
import pagent.identity
import prpc
from prouter.api import jobenv

from . import common


ROUTE_VARIABLE_CONNECTION_UID = 'conn_uid'
ROUTE_VARIABLE_JOB_UID = 'job_uid'


SCHEMA_JOB_CREATE = {
    'type': 'object',
    'properties': {
        'agent': {
            'oneOf': [
                {
                    'type': 'object',
                    'properties': {
                        'type': {
                            'type': 'string',
                            'enum': ['uid']
                        },
                        'uid': {'type': 'string'}
                    },
                    'additionalProperties': False,
                    'required': ['type', 'uid']
                },
                {
                    'type': 'object',
                    'properties': {
                        'type': {
                            'type': 'string',
                            'enum': ['address']
                        },
                        'address': {'type': 'string'},
                        'token': {'type': 'string'}
                    },
                    'additionalProperties': False,
                    'required': ['type', 'address', 'token']
                },
                {
                    'type': 'object',
                    'properties': {
                        'type': {
                            'type': 'string',
                            'enum': ['select']
                        },
                        'runtimes': {
                            'type': 'array',
                            'description': 'Possible runtimes, '
                                           'platform and jobenvs. '
                                           'May be empty.',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'uid': {'type': 'string'},
                                    'platform': {
                                        'type': 'array',
                                        'items': {
                                            'type': 'object',
                                            'description': 'Agent uname.'
                                        }
                                    },
                                    'jobenv': {
                                        'type': 'array',
                                        'items': {
                                            'type': 'object',
                                            'properties': {
                                                'guid': {'type': 'string'},
                                                'version': {'type': 'string'},
                                            },
                                            'additionalProperties': False,
                                            'required': ['guid', 'version']
                                        }
                                    }
                                },
                                'additionalProperties': False,
                                'required': ['uid', 'platform', 'jobenv']
                            }
                        }
                    },
                    'additionalProperties': False,
                    'required': ['type', 'runtimes'],
                }
            ]
        },
        'name': {'type': 'string'}
    },
    'additionalProperties': False,
    'required': ['agent', 'name']
}


SCHEMA_JOB_START = {
    'type': 'object',
    'properties': {
        'args': {
            'type': 'array',
            'items': {
                'type': 'string'
            },
            'minLength': 1
        },
        'env': {
            'type': 'object',
            'patternProperties': {
                '.*': {'type': 'string'}
            }
        },
        'cwd': {'oneOf': [{'type': 'string'}, {'type': 'null'}]},
        'port_expected_count': {
            'type': 'integer',
            'minValue': 0
        },
        'forward_stdout': {
            'type': 'boolean'
        }
    },
    'additionalProperties': False,
    'required': ['args', 'env']
}


def _extend_job_info(connection, info):
    """Extends job info with connection/API related data."""
    info['path'] = '/jobs/%s/%s' % (connection.id, info['uid'])
    info['agent'] = {
        'platform': connection.handshake_data[pagent.identity.KEY_PLATFORM],
        'properties': connection.handshake_data[pagent.identity.KEY_PROPERTIES]
    }
    return info


def _watch_active_connection(connection, polling_delay):
    """Create a watcher for the connection, automatically closing it
    if it has no running jobs.

    Should be used for outgoing (router->agent) connections only.
    """
    assert polling_delay >= 0
    async def connection_watcher():
        while connection.connected:
            if not connection.active:
                method = (
                    pagent.agent_service.
                    AgentService.job_count_current_connection
                )
                job_count = await connection.call_simple(method.__name__)
                if not job_count:
                    # Note: connection_unwatch is called directly from here,
                    # so shield protects connection._close
                    # from cancelling itself.
                    await asyncio.shield(connection.close())
                    return
            await asyncio.sleep(polling_delay)

    watcher_task = connection.loop.create_task(connection_watcher())

    async def connection_unwatch(connection):
        watcher_task.cancel()
        await asyncio.wait([watcher_task])

    connection.on_close.append(connection_unwatch)


async def job_create(request):
    """Create a new job on a given agent.

    Agent can be identified either by uid (it should be already connected)
    or by address (ip/hostname).
    """
    identity = request.app[common.KEY_IDENTITY]
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    request_data = await request.json()
    jsonschema.validate(request_data, SCHEMA_JOB_CREATE)
    job_name = request_data['name']
    agent_locator = request_data['agent']
    agent_locator_type = agent_locator['type']
    job_runtime = {}
    if agent_locator_type == 'uid':
        agent_uid = agent_locator['uid']
        connection = conn_manager.by_peer_uid(agent_uid)
    elif agent_locator_type == 'select':
        connections = {con.id: con for con in conn_manager.get_connections()
                       if con.mode == prpc.ConnectionMode.SERVER}
        hosts = [jobenv.Host(uid, con.handshake_data['platform'],
                             jobenv.search(con.handshake_data['properties']))
                 for uid, con in connections.items()]

        runtimes = []
        for rt in agent_locator['runtimes']:
            envs = [jobenv.jobenv_from_dict(env) for env in rt['jobenv']]
            runtimes.append(jobenv.Runtime(rt['uid'], rt['platform'], envs))

        host, host_jobenv, rt_uid = jobenv.select(hosts, runtimes)
        connection = connections[host.uid]
        if rt_uid:
            job_runtime['runtime'] = {'uid': rt_uid}
            if host_jobenv:
                job_runtime['runtime']['activate'] = host_jobenv.activate
    elif agent_locator_type == 'address':
        agent_address = agent_locator['address']
        agent_token = agent_locator['token']
        url = yarl.URL.build(
            scheme='http',
            host=agent_address,
            path=conn_manager.AGENT_RPC_PATH
        )

        async def on_connected(connection, handshake):
            conn_manager.register(connection, handshake)

        connection = prpc.Connection(
            loop=request.app.loop,
            debug=conn_manager.debug
        )
        await prpc.platform.ws_aiohttp.connect(
            connection,
            url,
            handshake_data=identity.get_client_handshake(agent_token),
            connect_callback=on_connected
        )
    else:
        raise ValueError('unknown agent locator type')

    try:
        info = await connection.call_simple(
            pagent.agent_service.AgentService.job_create.__name__, job_name
        )
    except:
        if connection.mode == prpc.ConnectionMode.CLIENT:
            await connection.close()
        raise

    if connection.mode == prpc.ConnectionMode.CLIENT:
        _watch_active_connection(connection, conn_manager.polling_delay)

    result = _extend_job_info(connection, info)
    result.update(job_runtime)
    return aiohttp.web.json_response(result)


async def job_remove(request):
    """Removes existing job."""
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[ROUTE_VARIABLE_JOB_UID]
    connection = conn_manager.connection(conn_uid)
    info = await connection.call_simple(
        pagent.agent_service.AgentService.job_remove.__name__, job_uid
    )
    return aiohttp.web.json_response(_extend_job_info(connection, info))


async def job_wait(request):
    """Wait for job completion."""
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[ROUTE_VARIABLE_JOB_UID]
    connection = conn_manager.connection(conn_uid)
    info = await connection.call_simple(
        pagent.agent_service.AgentService.job_wait.__name__, job_uid
    )
    return aiohttp.web.json_response(_extend_job_info(connection, info))


async def job_info(request):
    """Get job info."""
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[ROUTE_VARIABLE_JOB_UID]
    connection = conn_manager.connection(conn_uid)
    info = await connection.call_simple(
        pagent.agent_service.AgentService.job_info.__name__, job_uid
    )
    return aiohttp.web.json_response(_extend_job_info(connection, info))


async def job_start(request):
    """Start a process inside existing job."""
    conn_manager = request.app[common.KEY_CONN_MANAGER]
    conn_uid = request.match_info[ROUTE_VARIABLE_CONNECTION_UID]
    job_uid = request.match_info[ROUTE_VARIABLE_JOB_UID]
    request_data = await request.json()
    jsonschema.validate(request_data, SCHEMA_JOB_START)
    connection = conn_manager.connection(conn_uid)
    info = await connection.call_simple(
        pagent.agent_service.AgentService.job_start.__name__,
        job_uid,
        request_data['args'],
        request_data['env'],
        request_data.get('cwd', None),
        request_data.get('port_expected_count', 1),
        request_data.get('forward_stdout', False)
    )
    return aiohttp.web.json_response(_extend_job_info(connection, info))
