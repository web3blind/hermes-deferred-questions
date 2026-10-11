"""Exercise real pinned catalog clone + wheel install in a disposable Python venv.

Usage: HERMES_CORE_ROOT=/path/to/core TMPDIR=/path/to/scratch \
  /path/to/disposable/venv/bin/python scripts/verify_install.py

Requires Git, openssl, setuptools, pip and core/Telegram import dependencies in
that disposable venv. Uses only loopback HTTPS with a generated local CA. No
framework installer/loader is mocked, no remote publication or messages occur.
The source must be committed; external catalog metadata pins that commit.
"""
from __future__ import annotations
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading

ROOT = Path(__file__).resolve().parents[1]
CORE = Path(os.environ['HERMES_CORE_ROOT']).resolve()
assert sys.prefix != sys.base_prefix, 'Use a disposable venv, not system Python'
assert os.environ.get('TMPDIR'), 'Set TMPDIR to an explicit scratch directory'
# Refuse an installed Hermes environment; this test intentionally installs a wheel.
assert not Path(sys.prefix).resolve().is_relative_to(CORE), 'Do not use the core venv'
assert Path(sys.prefix).resolve().is_relative_to(Path(os.environ['TMPDIR']).resolve()), 'Disposable venv must be under TMPDIR'
assert not any(d.metadata.get('Name', '').replace('_', '-').lower() == 'hermes-deferred-questions'
               for d in importlib.metadata.distributions()), 'Use a fresh venv without this distribution'
sandbox = Path(tempfile.mkdtemp(prefix='dq-install-', dir=os.environ['TMPDIR']))
home = sandbox / 'home'
home.mkdir()
os.environ.update(HOME=str(home), HERMES_HOME=str(home / 'hermes'),
                  HERMES_BUNDLED_PLUGINS=str(sandbox / 'no-bundled-plugins'))
sys.path.append(str(CORE))
os.environ['PYTHONPATH'] = str(CORE)

def command(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout.strip()

sha = command('git', '-C', str(ROOT), 'rev-parse', 'HEAD')
assert len(sha) == 40
assert not command('git', '-C', str(ROOT), 'status', '--porcelain'), 'Commit reviewed source first'
served = sandbox / 'served'
served.mkdir()
bare = served / 'plugin.git'
command('git', 'clone', '--bare', str(ROOT), str(bare))
command('git', '-C', str(bare), 'update-server-info')
cert, key = sandbox / 'cert.pem', sandbox / 'key.pem'
command('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
        '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1',
        '-keyout', str(key), '-out', str(cert))
class QuietHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    def do_GET(self):
        self.backend()
    def do_POST(self):
        self.backend()
    def backend(self):
        from urllib.parse import urlsplit
        url = urlsplit(self.path)
        length = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(length) if length else b''
        env = dict(PATH=os.environ['PATH'], HOME=str(home),
                   GIT_PROJECT_ROOT=str(served), GIT_HTTP_EXPORT_ALL='1',
                   PATH_INFO=url.path, QUERY_STRING=url.query,
                   REQUEST_METHOD=self.command, CONTENT_TYPE=self.headers.get('Content-Type', ''),
                   CONTENT_LENGTH=str(length), REMOTE_USER='', REMOTE_ADDR='127.0.0.1',
                   SERVER_PROTOCOL='HTTP/1.1', SERVER_NAME='127.0.0.1')
        result = subprocess.run(['git', 'http-backend'], input=body, env=env,
                                capture_output=True, check=True)
        headers, payload = result.stdout.split(b'\r\n\r\n', 1)
        lines = headers.decode().split('\r\n')
        status = next((int(l.split(':', 1)[1].split()[0]) for l in lines if l.lower().startswith('status:')), 200)
        self.send_response(status)
        for line in lines:
            key, value = line.split(':', 1)
            if key.lower() != 'status':
                self.send_header(key, value.strip())
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
server = ThreadingHTTPServer(('127.0.0.1', 0), QuietHandler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(cert, key)
server.socket = context.wrap_socket(server.socket, server_side=True)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
os.environ['GIT_SSL_CAINFO'] = str(cert)
from hermes_cli.plugin_catalog import load_catalog
from hermes_cli.plugins_cmd_catalog import install_catalog_entry, read_catalog_sidecar
from hermes_cli.plugins_cmd import _read_install_metadata
from hermes_cli.plugins import PluginManager
catalog_dir = sandbox / 'catalog'
catalog_dir.mkdir()
repo = f'https://127.0.0.1:{server.server_port}/plugin.git'
# JSON is YAML-compatible; stays outside the pinned source tree.
source_entry, = load_catalog(ROOT / 'catalog')
entry_data = source_entry.to_dict()
entry_data.update(repo=repo, sha=sha)
(catalog_dir / 'deferred-questions.yaml').write_text(json.dumps(entry_data))
entry, = load_catalog(catalog_dir)
try:
    target, manifest, name = install_catalog_entry(entry, force=False)
    # No third-party runtime dependencies are declared. Do not rely on the
    # removed private dependency installer in supported Hermes releases.
    assert manifest.get('python_dependencies', []) == []
finally:
    server.shutdown()
    server.server_close()
    thread.join()
assert name == 'deferred-questions'
assert command('git', '-C', str(target), 'rev-parse', 'HEAD') == sha
record = _read_install_metadata()[name]
assert record['revision'] == sha and record['catalog']['sha'] == sha
assert read_catalog_sidecar(target)['sha'] == sha
from hermes_cli.plugin_validate import validate_plugin_dir
validation = validate_plugin_dir(target)
(sandbox / 'admission-report.json').write_text(json.dumps(validation.to_dict(), indent=2))
assert validation.ok, validation.failures
h = Path(os.environ['HERMES_HOME'])
h.mkdir(exist_ok=True)
h.joinpath('config.yaml').write_text('plugins:\n  enabled: [deferred-questions]\n')

async def exercise(source):
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from gateway.config import PlatformConfig
    from telegram.ext import Application
    manager = PluginManager(scope_key=str(h))
    manager.discover_and_load()
    loaded = manager._plugins[name]
    assert loaded.enabled, loaded.error
    assert loaded.manifest.source == source
    module = importlib.import_module('hermes_deferred_questions')
    rt = manager._hooks['pre_gateway_dispatch'][0].__self__
    adapter = TelegramAdapter(PlatformConfig(enabled=True))
    native = Application.builder().token('123:test').build()
    adapter._running = False
    # Exercise real adapter connect-time factory seam BEFORE readiness.
    from unittest.mock import patch
    with patch('hermes_cli.plugins.get_plugin_manager', return_value=manager):
        adapter._wire_plugin_handlers(native)
    assert rt.adapters['telegram'] == (native, adapter)
    retired = [rt]
    for _ in range(2):
        await asyncio.to_thread(manager.discover_and_load, force=True)
        retired.append(manager._hooks['pre_gateway_dispatch'][0].__self__)
        await asyncio.sleep(.05)
    fresh = retired.pop()
    assert importlib.import_module('hermes_deferred_questions') is module
    await asyncio.sleep(1.1)
    assert not adapter.is_connected and fresh.adapters == {}
    assert not native.handlers.get(-20)
    adapter._running = True
    for _ in range(200):
        if fresh.adapters.get('telegram') == (native, adapter):
            break
        await asyncio.sleep(.01)
    assert fresh.adapters.get('telegram') == (native, adapter)
    assert fresh.health()['telegram']['worker_alive']
    assert len(native.handlers[-20]) == 1
    assert all(not old.live for old in retired)
    assert all(t.done() for old in retired for tasks in old._tasks.values() for t in tasks)
    manager.unload()
    await asyncio.sleep(.1)
    assert not native.handlers.get(-20)
    return module

module = asyncio.run(exercise('user'))
# Build the actual installed clone, then install its wheel in this disposable venv.
wheels = sandbox / 'wheels'
wheels.mkdir()
command(sys.executable, '-m', 'pip', 'wheel', '--no-deps', '--no-build-isolation',
        '--wheel-dir', str(wheels), str(target))
wheel, = wheels.glob('*.whl')
command(sys.executable, '-m', 'pip', 'install', '--no-deps', str(wheel))
# Remove directory precedence and cached source import; prove the installed wheel.
shutil.move(str(target), str(sandbox / 'retired-directory'))
sys.path[:] = [p for p in sys.path if p != str(target)]
for key in tuple(sys.modules):
    if key == 'hermes_deferred_questions' or key.startswith('hermes_deferred_questions.'):
        del sys.modules[key]
ep, = [e for e in importlib.metadata.entry_points(group='hermes_agent.plugins') if e.name == name]
installed = ep.load()
assert Path(installed.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
for filename in ('__init__.py', 'runtime.py', 'store.py', 'lifecycle.py', 'plugin.yaml'):
    assert Path(installed.__file__).parent.joinpath(filename).read_bytes() == ROOT.joinpath('hermes_deferred_questions', filename).read_bytes()
assert asyncio.run(exercise('entrypoint')) is installed
print(json.dumps(dict(result='passed', sha=sha, catalog_clone=True,
                      directory_force_before_readiness=True, wheel_installed=True,
                      entrypoint_force_before_readiness=True, scratch=str(sandbox))))
