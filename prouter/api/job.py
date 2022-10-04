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

import io
import pathlib
import shutil
import stat
import tarfile
import tempfile
import uuid

from prouter import control_app


class Job(object):
    """ Router job interface. Intended to simplify interaction with pRouter.
        Implements async context manager interface.
    Args:
        router: router instance.
        name: job name.
        uid: agent uid, either uid or address and token must be specified.
        address: agent address.
        token: agent access token.
        runtimes: list of agent runtimes for automatic agent selection.
    """
    def __init__(self, router, name,
                 uid=None, address=None, token=None, runtimes=None):
        assert address is None == token is None, (
            'agent address and token must be speicified or omited together')
        assert not (bool(uid) and bool(address)), (
            'agent uid and agent address must not be specified together')

        self._router = router
        self._job = None
        agent = {}
        if uid:
            agent['type'] = 'uid'
            agent['uid'] = uid
        elif address and token:
            agent['type'] = 'address'
            agent['address'] = address
            agent['token'] = token
        else:
            agent['type'] = 'select'
            agent['runtimes'] = runtimes
        self._args = {'json': {'name': name, 'agent': agent}}

    @property
    def properties(self):
        """ Returns: static job properties. """
        return self._job

    async def info(self):
        """ Returns: information about job as json. """
        return await self._request('POST', 'info')

    async def wait(self):
        """ Wait until job finished. """
        return await self._request('POST', 'wait')

    async def start(self, args, **params):
        """ Start job executable.
        Args:
            args: command line.
            params: job keyword arguments.
        Returns: information about started job as json.
        """
        if 'args' in params:
            raise ValueError('start arguments in keywords')
        params['env'] = params.get('env', {})
        request_kwargs = {'json': params}
        request_kwargs['json']['args'] = args
        return await self._request('POST', 'start', kwargs=request_kwargs)

    async def upload(self, target, source, tmpdir=None, **params):
        """ Upload file or directory to job directory.
        Args:
            target: destination path must be relative.
            source: upload target, string interpreted as file or directory path
                bytes-like objects are uploaded as file content
            tmpdir: temporary directory where upload archive is created.
            params: additional request parameters (i.e. executable=1).
        """
        target = pathlib.Path(target)
        kwargs = {
            'headers': {
                'Content-Type': 'application/octet-stream',
                'Content-Length': None
            },
            'data': None
        }
        if params:
            kwargs['params'] = {k: str(v) for k, v in params.items()}

        ## TODO: what about io.StringIO ?
        if isinstance(source, (bytes, bytearray, io.BytesIO)):
            if not isinstance(source, io.BytesIO):
                source = io.BytesIO(source)
            source.seek(0, io.SEEK_END)
            kwargs['headers']['Content-Length'] = str(source.tell())
            source.seek(0, io.SEEK_SET)
            kwargs['data'] = source
            return await self._request('POST', 'file/%s' % (target,),
                                       result=False, kwargs=kwargs)

        source = pathlib.Path(source)
        if not source.exists():
            raise ValueError('upload source not found: \'%s\'' % (source,))

        if source.is_file():
            file_stat = source.stat()
            if file_stat.st_mode & stat.S_IEXEC:
                if 'params' not in kwargs:
                    kwargs['params'] = {}
                kwargs['params']['executable'] = '1'
            with source.open('rb') as file:
                kwargs['data'] = file
                kwargs['headers']['Content-Length'] = str(file_stat.st_size)
                return await self._request('POST', 'file/%s' % (target,),
                                           result=False, kwargs=kwargs)

        create_tmp = not tmpdir
        tmpdir = pathlib.Path(tempfile.mkdtemp() if create_tmp else tmpdir)
        arcpath = None
        try:
            arcpath = tmpdir.joinpath('%s.tar' % (uuid.uuid4().hex,))
            with tarfile.open(arcpath, 'w') as arc:
                for item in source.iterdir():
                    arc.add(item, arcname=target.joinpath(item.name))
            kwargs['headers']['Content-Length'] = str(arcpath.stat().st_size)
            with open(arcpath, 'rb') as arc:
                kwargs['data'] = arc
                return await self._request(
                    'POST', 'archive', result=False, kwargs=kwargs
                )
        finally:
            if create_tmp:
                shutil.rmtree(tmpdir)
            else:
                arcpath.unlink()

    async def download(self, target):
        """ Download specified file from job directory.
        Args:
            target: path to download target, must be relative.
        Returns: downloaded data as bytearray.
        """
        url = self._router.url.with_path(self._path('file/%s' % target))
        async with self._router.session.request('GET', url) as response:
            response.raise_for_status()
            buffer = io.BytesIO()
            chunk = await response.content.readany()
            while chunk:
                buffer.write(chunk)
                chunk = await response.content.readany()
            return buffer.getvalue()

    async def download_archive(
            self, target, exclude=None, tmpdir=None, destination=None):
        """ Download files (or directories) from job directory as tar archive.
            Optionally uppack archive to destination directory.
        Args:
            target: relative path to target, may be filemask.
            exclude: exclude files from resulting archive.
            tmpdir: temporary directory where archive will be created.
            destination: destination directory for unpacking archive,
                either tmpdir or destination must be specified.
        Returns: path to created archive if destination is omited.
        """
        assert tmpdir is not None or destination is not None
        if destination is not None:
            destination = pathlib.Path(destination)
            if destination.is_file():
                raise ValueError('archive unpacking destination is a file')
            destination.mkdir(parents=True, exist_ok=True)

        arcpath = pathlib.Path(tmpdir) if destination is None else destination
        arcpath = arcpath.joinpath('%s.tar' % (uuid.uuid4().hex,))
        try:
            with open(arcpath, 'wb') as arc:
                url = self._router.url.with_path(self._path('archive'))
                params = {'include': str(target)}
                if exclude:
                    params['exclude'] = str(exclude)
                response = await self._router.session.request(
                    'GET', url, params=params
                )
                async with response:
                    response.raise_for_status()
                    chunk = await response.content.readany()
                    while chunk:
                        arc.write(chunk)
                        chunk = await response.content.readany()
            if destination is None:
                return arcpath
            with tarfile.open(arcpath, 'r') as arc:
                
                import os
                
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(arc, destination)
            arcpath.unlink()
        except Exception:
            if arcpath.exists():
                arcpath.unlink()
            raise

    async def http(self, method, path, **kwargs):
        """ Send http request to job. """
        url = self._router.url.with_path(self._path('http/%s' % path))
        return await self._router.session.request(method, url, **kwargs)

    async def ws(self, path, **kwargs):
        """ Open websocket connection with job. """
        url = self._router.url.with_path(self._path('http/%s' % path))
        return await self._router.session.ws_connect(url, **kwargs)

    async def ws_loopback(self, path, loopback_url):
        kwargs = {'json': {'url': str(loopback_url)}}
        await self._request('POST', 'wsconnect/%s' % path, False, kwargs)

    def _path(self, path):
        assert self._job
        return '%s/%s' % (self._job['path'], path)

    async def _request(self, method, path, result=True, kwargs=None):
        return await self._router.request(
            method, self._path(path), result, kwargs or {}
        )

    async def __aenter__(self):
        self._job = await self._router.request(
            'POST', control_app.ROUTE_JOB_CREATE, kwargs=self._args
        )
        return self

    async def __aexit__(self, *exc_info):
        await self._request('POST', 'remove')
        self._job = None
