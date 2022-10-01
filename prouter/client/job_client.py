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

"""Router job client."""

import io
import pathlib
import shutil
import stat
import tarfile
import tempfile


class JobClient(object):
    """Router job client.

    Intended to simplify interaction with router jobs. Communicates with
    the router through its HTTP interface. May create either a new job
    or attach to an existing one.

    Instances implement async context manager interface. NOTE: The job
    is removed from the router when control leaves the contex. To keep
    the job alive call `keep`.

    Do not construct instances explicitly. Call one of the following
    factory methods instead:
        create: Create new router job.
        attach: Connect to the exiting router job.
    """

    # --------------------------------------------------------- FACTORY METHODS

    @classmethod
    async def create(cls, router, arguments):
        """Create new router job.

        Args:
            router: Router client instance.
            arguments: New router job arguments.
        Returns:
            JobClient instance representing new router job.
        """
        # Access to protected fields allowed from the factory method.
        # pylint: disable=protected-access

        # Create and initialize job client instance.
        self = JobClient(router, cls.__init_guard)
        self._info = await self._router.request('POST', '/jobs/create',
                                                kwds=arguments)
        return self

    @classmethod
    async def attach(cls, router, job_path):
        """Attach to existing router job.

        Args:
            job_path: Path identifying the job.
        Returns:
            JobClient instance representing the router job attached.
        """

        # Access to protected fields allowed from the factory method.
        # pylint: disable=protected-access

        # Create and initialize job client instance.
        self = JobClient(router, cls.__init_guard)
        self._info = await self._router.request('GET', f'{job_path}/info')
        return self

    # --------------------------------------------------------- CONTEXT MANAGER

    async def __aenter__(self):
        # Just in case update the job info.
        await self.update_info()
        return self

    async def __aexit__(self, *exc_info):
        # Remove the job if `keep` is not called.
        if not self._keep_on_exit:
            await self._request('POST', 'remove')
            self._keep_on_exit = False

    async def keep(self):
        """Do not remove job when control leaves the context."""
        self._keep_on_exit = True

    # ------------------------------------------------------- USEFUL PROPERTIES

    @property
    def info(self):
        """Job info dict stored in the job client instance."""
        return self._info

    @property
    def path(self):
        """Job path on the router, unambiguously identifies the job."""
        return self._info['path']

    # -------------------------------------------------------- JOB API REQUESTS

    async def update_info(self):
        """Update the job info.

        Returns:
            The job info dict.
        """
        self._info = await self._request('GET', 'info')
        return self._info

    async def wait(self):
        """Wait the job to finish (block until it finishes)."""
        return await self._request('POST', 'wait')

    async def start(self, args, **params):
        """Start job executable.

        Args:
            args: Command line arguments.
            params: Job keyword arguments.
        Returns:
            Job info.
        """

        params['env'] = params.get('env', {})
        start_kwds = {'json': params}
        start_kwds['json']['args'] = args
        self._info = await self._request('POST', 'start', kwds=start_kwds)
        return self._info

    async def upload(self, target, source, **params):
        """Upload file or directory to the job directory.

        Args:
            target: Destination path. Must be relative.
            source: Upload source, either string or bytes-like object.
                String interpreted as a file or a directory path.
                Bytes-like objects are uploaded as file content.
            params: Additional request parameters (e.g. executable=1).
        """

        target = pathlib.Path(target)
        kwds = {
            'headers': {
                'Content-Type': 'application/octet-stream',
                'Content-Length': None
            },
            'data': None
        }
        if params:
            kwds['params'] = {k: str(v) for k, v in params.items()}

        # Is source is a bytes-like object - upload it as a content.
        if isinstance(source, (bytes, bytearray, io.BytesIO)):
            if not isinstance(source, io.BytesIO):
                source = io.BytesIO(source)
            source.seek(0, io.SEEK_END)
            kwds['headers']['Content-Length'] = str(source.tell())
            source.seek(0, io.SEEK_SET)
            kwds['data'] = source
            return await self._request('POST', f'file/{str(target)}',
                                       result=False, kwds=kwds)

        # Consider that `source` is a path.
        source = pathlib.Path(source)
        if not source.exists():
            raise ValueError(f'Upload source not found: `{str(source)}`!')

        if source.is_file():
            file_stat = source.stat()
            if file_stat.st_mode & stat.S_IEXEC:
                if 'params' not in kwds:
                    kwds['params'] = {}
                kwds['params']['executable'] = '1'
            with source.open('rb') as file:
                kwds['data'] = file
                kwds['headers']['Content-Length'] = str(file_stat.st_size)
                return await self._request('POST', f'file/{target}',
                                           result=False, kwds=kwds)

        with tempfile.TemporaryDirectory() as tempdir:
            arcpath = pathlib.Path(tempdir).joinpath('archive.tar')
            with tarfile.open(arcpath, 'w') as arc:
                for item in source.iterdir():
                    arc.add(item, arcname=target.joinpath(item.name))
            kwds['headers']['Content-Length'] = str(
                arcpath.stat().st_size)  # pylint: disable=no-member
            with open(arcpath, 'rb') as arc:
                kwds['data'] = arc
                return await self._request(
                    'POST', 'archive', result=False, kwds=kwds)

    async def download_file(self, source, destination=None):
        """Download file from job working directory. May return file
        content or copy file to the specified location.
            If destination is `None` file content returned.
            If destination is an existing file it will be overwritten.
            If destination is an existing directory file with the same
                name as source will be created in it.
            If destination does not exist it is treated as file but also
                all parent directories will be created.
        Args:
            source (str): relative path of the file to download.
            destination (str or None): destination path or None.
        Returns:
            Downloaded data as `bytes` if destination is omitted."""
        url = self._router.url.with_path(self._path('file/%s' % source))

        async def _download(target):
            async with self._router.session.request('GET', url) as response:
                response.raise_for_status()
                chunk = await response.content.readany()
                while chunk:
                    target.write(chunk)
                    chunk = await response.content.readany()

        if destination is None:
            buffer = io.BytesIO()
            await _download(buffer)
            return buffer.getvalue()

        destination = pathlib.Path(destination)
        if destination.is_dir():
            destination = destination.joinpath(pathlib.Path(source).name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open('wb') as file:
            await _download(file)

    async def download_directory(self, source, destination, exclude=None):
        """Download directory from job working directory. All content of
        the source directory will be copied to the destinaion directory.
        If destination directory does not exist it will be created.
        Args:
            source (str): relative path to the directory to download.
            destination (str): path to the destination directory.
            exclude (str): files to ignore, wildcards supported."""
        destination = pathlib.Path(destination)
        if destination.is_file():
            raise ValueError('Can not download directory to a file!')

        with tempfile.TemporaryDirectory() as tempdir:
            arcpath = pathlib.Path(tempdir).joinpath('archive.tar')
            with open(arcpath, 'wb') as arc:
                url = self._router.url.with_path(self._path('archive'))
                # router uses wildcards to include files in archive
                params = {'include': str(source) + '/*'}
                if exclude:
                    params['exclude'] = str(exclude)
                response = await self._router.session.request(
                    'GET', url, params=params)
                async with response:
                    response.raise_for_status()
                    chunk = await response.content.readany()
                    while chunk:
                        arc.write(chunk)
                        chunk = await response.content.readany()

            # router makes archive from job working directory and tar
            # archive will contain source path, this means we can not
            # unpack archive directly to destination directory and must
            # move files to correct locations after unpacking
            unpack_path = pathlib.Path(tempdir).joinpath('unpacked')
            unpack_path.mkdir()  # pylint: disable=no-member
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
                
                    tar.extractall(path, members, numeric_owner) 
                    
                
                safe_extract(arc, unpack_path)
            unpack_path = unpack_path.joinpath(source)
            for src in unpack_path.glob('**/*'):  # pylint: disable=no-member
                dst = destination.joinpath(src.relative_to(unpack_path))
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.is_dir():
                    dst.mkdir(exist_ok=True)
                else:
                    if dst.exists():
                        dst.unlink()
                    shutil.move(str(src), str(dst.parent))

    async def http(self, method, path, **kwds):
        """Make HTTP request to the router job."""
        url = self._router.url.with_path(self._path(f'http/{path}'))
        return await self._router.session.request(method, url, **kwds)

    async def ws(self, path, **kwds):
        """Open websocket connection to the job."""
        url = self._router.url.with_path(self._path(f'http/{path}'))
        return await self._router.session.ws_connect(url, **kwds)

    async def ws_bridge(self, job_url_path, loopback_url):
        """Make a WS bridge connection between job and a given endpoint.

        After this call an agent will connect to the job (with URL path
        `job_url_path`) and the router will connect to the given
        `loopback_url`. As a result the WebSocket connection between two
        servers (!) will be established.

        Args:
            job_url_path: Path part of the URL agent will connect to.
            loopback_url: The endpoint router will connect to.
        """
        kwds = {'json': {'url': str(loopback_url)}}
        await self._request('POST', f'wsconnect/{job_url_path}', False, kwds)

    # ------------------------------------------------------------- AUXILIARIES

    def _path(self, path):
        """Shortcut to make a job URL path."""
        return f'{self._info["path"]}/{path}'

    async def _request(self, method, path, result=True, kwds=None):
        """Make request to the job."""
        return await self._router.request(method, self._path(path), result,
                                          kwds or {})

    # ----------------------------------------------------- PRIVATE CONSTRUCTOR

    # Auxiliary guard object to avoid sudden constructor calls.
    __init_guard = object()

    def __init__(self, router, *args):
        """Never call from outside, use `attach` or `create` instead.

        Args:
            router: Router client instance.
        """

        assert args and args[0] is self.__init_guard, (
            'Do not create instances of `JobClient` directly, use '
            '`attach` or `create` methods instead!'
        )

        # Router client instance.
        self._router = router
        # Dict with router job info.
        self._info = None
        # Flag which set in `detach` to avoid removing job when
        # control leaves the context manager.
        self._keep_on_exit = False
