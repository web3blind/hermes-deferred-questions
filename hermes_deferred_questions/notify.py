"""Profile-local wake notices. No bot connection, polling or job scheduler."""
from __future__ import annotations

import asyncio
from concurrent.futures import Future
import fcntl
import json
import os
from pathlib import Path
import socket
import stat
import tempfile
import threading


NAME = 'events.sock'


def notify(home):
    """Persisted work survives a failed notice; callers receive an honest receipt."""
    folder = Path(home).resolve() / 'deferred-questions'
    try:
        record = json.loads((folder/'events-address.json').read_text())
        if record.get('home') != str(Path(home).resolve()):
            return False
        path = Path(record['path'])
        if path.parent.stat().st_uid != os.getuid() or path.parent.stat().st_mode & 0o077:
            return False
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1)
            client.connect(str(path))
            client.sendall(b'wake\n')
            reply = bytearray()
            while len(reply) < 32 and not reply.endswith(b'\n'):
                chunk = client.recv(32 - len(reply))
                if not chunk:
                    break
                reply.extend(chunk)
            return bytes(reply) == b'accepted\n'
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return False


class Endpoint:
    """One leased local listener per profile. Lease inode is never removed."""
    def __init__(self, home, wake):
        self.folder = Path(home) / 'deferred-questions'
        self.path = self.folder / NAME
        self.wake = wake
        self.server = None
        self.lease = None
        self.identity = None
        self.clients = set()
        self.live = True
        self.loop = None
        self.temporary = None
        self.starting = None
        self.ready = None
        self.guard = threading.RLock()

    async def start(self):
        with self.guard:
            if not self.live:
                return
            starter = self.ready is None
            if starter:
                self.ready = Future()
                self.loop = asyncio.get_running_loop()
                self.starting = asyncio.current_task()
            ready = self.ready
        if not starter:
            await asyncio.shield(asyncio.wrap_future(ready))
            return
        try:
            lease = (self.folder / 'events.lock').open('a+b')
            try:
                os.fchmod(lease.fileno(), 0o600)
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BaseException:
                lease.close()
                raise
            self.lease = lease
            self.folder.chmod(0o700)
            if len(os.fsencode(self.path)) >= 100:
                self.temporary = Path(tempfile.mkdtemp(prefix='dq-'))
                self.path = self.temporary / 's'
            if self.path.exists() or self.path.is_symlink():
                if self.path.is_symlink() or not stat.S_ISSOCK(self.path.stat().st_mode):
                    raise RuntimeError('question notification path is not a socket')
                self.path.unlink()
            self.server = await asyncio.start_unix_server(self._receive, path=str(self.path))
            self.path.chmod(0o600)
            st = self.path.stat()
            self.identity = (st.st_dev, st.st_ino)
            if not self.live:
                raise asyncio.CancelledError
            record = json.dumps({'home': str(self.folder.parent.resolve()), 'path': str(self.path)})
            temporary = self.folder / '.events-address.tmp'
            with temporary.open('w') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write(record)
            os.replace(temporary, self.folder/'events-address.json')
            ready.set_result(True)
        except BaseException as exc:
            if not ready.done():
                ready.set_exception(exc)
            self._close()
            raise
        finally:
            with self.guard:
                self.starting = None

    async def _receive(self, reader, writer):
        task = asyncio.current_task()
        self.clients.add(task)
        try:
            message = await asyncio.wait_for(reader.readexactly(5), timeout=1)
            if self.live and message == b'wake\n':
                self.wake()
                writer.write(b'accepted\n')
                await writer.drain()
        except (OSError, asyncio.IncompleteReadError, TimeoutError):
            pass
        finally:
            self.clients.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    def close(self):
        with self.guard:
            self.live = False
            loop = self.loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._request_close)
        else:
            self._request_close()

    def _request_close(self):
        if self.starting is not None and not self.starting.done():
            self.starting.cancel()
        else:
            self._close()

    def _close(self):
        if self.server is not None:
            self.server.close()
            self.server = None
        for task in tuple(self.clients):
            task.cancel()
        if self.identity is not None:
            try:
                st = self.path.lstat()
                if (st.st_dev, st.st_ino) == self.identity:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            self.identity = None
        if self.lease is not None:
            pointer = self.folder/'events-address.json'
            try:
                if json.loads(pointer.read_text()).get('path') == str(self.path):
                    pointer.unlink()
            except (OSError, ValueError, AttributeError):
                pass
            self.lease.close()
            self.lease = None
        if self.temporary is not None:
            try:
                self.temporary.rmdir()
            except OSError:
                pass
            self.temporary = None
