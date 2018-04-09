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

import pathlib
import re
from collections import namedtuple
from random import randint

import yaml


JobEnv = namedtuple('JobEnv', ['guid', 'version', 'activate'])
Host = namedtuple('Host', ['uid', 'platform', 'jobenvs'])
Runtime = namedtuple('Runtime', ['uid', 'platforms', 'jobenvs'])


def search(path_or_evironment):
    """ Returns: list of available jobenvs. """
    result = []
    if isinstance(path_or_evironment, dict):
        for var, value in path_or_evironment.items():
            match = _ENV_VARIABLE_REGEX.match(var)
            if match:
                result.append(JobEnv(match[1], _version(match[2]), value))
        return result

    for path in pathlib.Path(path_or_evironment).glob('*/%s' % (_MANIFEST,)):
        with open(path, mode='r', encoding='utf-8') as file:
            manifest = yaml.load(file, Loader=yaml.CSafeLoader)
            if manifest.get(_MANIFEST_TYPE_KEY, None) != _MANIFEST_TYPE:
                continue
            if manifest.get(_MANIFEST_VERSION_KEY, None) != _MANIFEST_VERSION:
                continue
            if not all([key in manifest
                        for key in ['activate', 'guid', 'version']]):
                continue
            activate = path.parent.absolute().joinpath(manifest['activate'])
            if not activate.is_file():
                continue
            result.append(JobEnv(
                manifest['guid'], _version(manifest['version']), activate))
    return result

def select(hosts, runtimes):
    """ Search for a suitable host for a job with required runtimes.
        Returns: host, its jobenv and runtime uid """
    if not runtimes:
        return hosts[randint(0, len(hosts) - 1)], None, None

    selected = {}
    for host in hosts:
        for rt in runtimes:
            matched, jobenv = _runtime_match(rt, host)
            if matched:
                selected[host.uid] = (host, jobenv, rt.uid)
                break
    if not selected:
        raise ValueError('failed to select host for required runtimes')

    ids = list(selected.keys())
    return selected[ids[randint(0, len(ids) - 1)]]

def jobenv_from_dict(data):
    """ Returns: JobEnv object created from dictionary. """
    return JobEnv(
        data['guid'], _version(data['version']), data.get('activate', None))


_ENV_VARIABLE_REGEX = re.compile(r'^JOBENV__(.+?)__(\d+?\.\d+?.+?)$')
_MANIFEST = 'manifest.yaml'
_MANIFEST_TYPE_KEY = 'manifest_type'
_MANIFEST_TYPE = 'jobenv'
_MANIFEST_VERSION_KEY = 'manifest_version'
_MANIFEST_VERSION = '1.0.0'


def _version(version):
    return tuple(map(int, version.split('.'))
                 if isinstance(version, str) else version)

def _jobenv_match(host, required):
    return (host.guid == required.guid and
            host.version[0] == required.version[0] and
            host.version >= required.version)

def _runtime_match(runtime, host):
    if runtime.platforms:
        for platform in runtime.platforms:
            for param, value in platform.items():
                if host.platform.get(param, None) != value:
                    break
            else:
                # runtime platform is matched with host
                break
        else:
            # host does not match anything in runtime
            return False, None
    if not runtime.jobenvs:
        return True, None

    # @TODO: nested cycles are bad
    for required_env in runtime.jobenvs:
        for host_env in host.jobenvs:
            if _jobenv_match(host_env, required_env):
                return True, host_env
    return False, None
