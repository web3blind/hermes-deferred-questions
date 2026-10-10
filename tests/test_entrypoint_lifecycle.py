"""Official metadata loader and native adapter lifecycle acceptance, no network."""
import asyncio
import importlib
import sys

import pytest
from test_deferred_questions import runtime, PROJECT


@pytest.fixture
def ep(tmp_path, monkeypatch):
    home = tmp_path / 'home'
    home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setenv('HOME', str(tmp_path))
    home.joinpath('config.yaml').write_text('plugins:\n  enabled: [deferred-questions]\n')
    # Ordinary importable package and genuine metadata EntryPoint.load, no alias.
    import shutil
    package = tmp_path / 'hermes_deferred_questions'
    package.mkdir()
    for name in ('__init__.py', 'runtime.py', 'store.py', 'lifecycle.py'):
        if (PROJECT / 'hermes_deferred_questions' / name).exists():
            shutil.copy2(PROJECT / 'hermes_deferred_questions' / name, package / name)
    metadata = tmp_path / 'hermes_deferred_questions-0.1.1.dist-info'
    metadata.mkdir()
    metadata.joinpath('METADATA').write_text('Name: hermes-deferred-questions\nVersion: 0.1.1\n')
    metadata.joinpath('entry_points.txt').write_text('[hermes_agent.plugins]\ndeferred-questions = hermes_deferred_questions\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    # Host entrypoint discovery may have pre-imported the installed distribution.
    # Exercise this isolated source copy even for the first test in a subprocess.
    for name in tuple(sys.modules):
        if name == 'hermes_deferred_questions' or name.startswith('hermes_deferred_questions.'):
            del sys.modules[name]
    from hermes_cli.plugins import PluginManager
    manager = PluginManager(scope_key=str(home))
    manager.discover_and_load()
    assert manager._plugins['deferred-questions'].enabled
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    yield manager, importlib.import_module('hermes_deferred_questions'), metadata
    manager.unload()
    for name in tuple(sys.modules):
        if name == 'hermes_deferred_questions' or name.startswith('hermes_deferred_questions.'):
            del sys.modules[name]


def adapter_for(platform):
    from gateway.config import PlatformConfig
    if platform == 'telegram':
        from plugins.platforms.telegram.adapter import TelegramAdapter
        adapter = TelegramAdapter(PlatformConfig(enabled=True))
        from telegram.ext import Application
        native = Application.builder().token('123:test').build()
    else:
        from plugins.platforms.vk.adapter import VKAdapter
        adapter = VKAdapter(PlatformConfig(enabled=True))
        native = adapter
    adapter._running = True
    return adapter, native


@pytest.mark.asyncio
@pytest.mark.parametrize('platform', ['telegram', 'vk'])
@pytest.mark.parametrize('offloop', [False, True])
async def test_force_waits_for_async_finalizer_without_manual_rewire(ep, monkeypatch, platform, offloop):
    manager, module, _ = ep
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    from gateway.run_plugin_rewire import GatewayPluginRewireMixin
    from gateway.config import Platform
    runner = GatewayPluginRewireMixin()
    old = runtime(manager)
    started, finishing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    active = 0
    highwater = 0
    async def worker(self, p):
        nonlocal active, highwater
        active += 1
        highwater = max(highwater, active)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finishing.set()
            await release.wait()
            active -= 1
    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    adapter, native = adapter_for(platform)
    runner.adapters = {Platform(platform): adapter}
    runner._subscribe_plugin_rewire(manager)
    adapter._wire_plugin_handlers(native)
    await asyncio.wait_for(started.wait(), 2)
    if offloop:
        await asyncio.to_thread(manager.discover_and_load, force=True)
    else:
        manager.discover_and_load(force=True)
    fresh = runtime(manager)
    await asyncio.wait_for(finishing.wait(), 2)
    await asyncio.sleep(.05)
    assert not old.live
    assert platform not in fresh.adapters
    assert highwater == 1
    release.set()
    for _ in range(100):
        if platform in fresh.adapters:
            break
        await asyncio.sleep(.01)
    assert platform in fresh.adapters
    assert highwater == 1
    adapter.rewire_plugin_handlers()
    if platform == 'telegram':
        assert len(native.handlers[-20]) == 1
    manager.unload()
    await asyncio.sleep(.05)
    assert active == 0
    assert all(t.done() for t in old.workers.values())
    for unsubscribe in runner._plugin_rewire_unsubscribe.values():
        unsubscribe()


@pytest.mark.asyncio
async def test_bad_source_row_does_not_poison_following_rows_or_continuation(ep, monkeypatch):
    from test_deferred_questions import request, ControlledAdapter
    manager, module, _ = ep
    rt = runtime(manager)
    adapter = ControlledAdapter.make('signal')
    async def idle(self, platform):
        await asyncio.Event().wait()
    monkeypatch.setattr(module.runtime.Runtime, 'worker', idle)
    rt.wire('signal', None, adapter)
    first = rt.store.create(request('signal', idempotency_key='bad'), rt.platforms)
    second = rt.store.create(request('signal', idempotency_key='good'), rt.platforms)
    calls = 0
    original = rt.source
    def source(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('private-recipient-secret')
        return original(*args, **kwargs)
    monkeypatch.setattr(rt, 'source', source)
    await rt.dispatch_once('signal')
    assert rt.store.get(first['id'])['state'] == 'queued'
    assert rt.store.get(second['id'])['state'] == 'sent'
    assert rt.health()['signal']['reason'] == 'dispatch_source_failed'
    assert 'private' not in repr(rt.health())
