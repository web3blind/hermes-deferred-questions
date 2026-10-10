"""Characterize the real host seam; these are NOT recovery acceptance tests.

No transport connects or live-home writes. If host lifecycle behavior is fixed,
update these limitation assertions and add recovery acceptance tests first.
"""
import asyncio
import importlib
import shutil
import sys
import threading
from types import SimpleNamespace as NS

import pytest

from test_deferred_questions import PROJECT, runtime


def adapters(manager, monkeypatch):
    from gateway.config import Platform, PlatformConfig
    from gateway.run_plugin_rewire import GatewayPluginRewireMixin
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from plugins.platforms.vk.adapter import VKAdapter

    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    telegram = TelegramAdapter(PlatformConfig(enabled=True))
    vk = VKAdapter(PlatformConfig(enabled=True))
    handlers = []
    cleanup_threads = []

    def remove_handler(handler, **kw):
        cleanup_threads.append(threading.get_ident())
        handlers.remove(handler)

    native = NS(
        add_handler=lambda handler, **kw: handlers.append(handler),
        remove_handler=remove_handler,
        cleanup_threads=cleanup_threads,
    )
    runner = GatewayPluginRewireMixin()
    runner.adapters = {Platform.TELEGRAM: telegram, Platform('vk'): vk}
    runner._subscribe_plugin_rewire(manager)
    telegram._wire_plugin_handlers(native)
    vk._wire_plugin_handlers(None)
    return runner, telegram, vk, native, handlers


async def settle():
    # Bounded loop turns, not a wall-clock wait or background polling service.
    for _ in range(4):
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize('off_loop', [False, True], ids=['on-loop', 'tool-thread'])
async def test_tts_force_has_no_automatic_rebind_same_clients(installed, monkeypatch, off_loop):
    from tools.tts_tool_plugins import _lookup_plugin_provider

    manager, module, _ = installed
    runner, telegram, vk, native, handlers = adapters(manager, monkeypatch)
    old = runtime(manager)
    module = sys.modules[old.__class__.__module__.removesuffix('.runtime')]
    await settle()
    assert set(old.adapters) == {'telegram', 'vk'}
    assert len(handlers) == 1
    events = []
    unsubscribe = manager.on_plugin_loaded(events.append)
    try:
        if off_loop:
            result = await asyncio.to_thread(
                _lookup_plugin_provider, 'isolated-lifecycle-missing-provider', retry=True)
        else:
            result = _lookup_plugin_provider('isolated-lifecycle-missing-provider', retry=True)
        await settle()
        fresh = runtime(manager)
        assert result is None
        assert not old.live and old.adapters == {} and old.cleanups == []
        assert all(task.done() for task in old.workers.values())
        assert handlers == []
        assert len(native.cleanup_threads) == 1
        assert native.cleanup_threads[0] == threading.get_ident()
        assert fresh is not old and fresh.live
        assert events == []  # Gateway subscription exists but host does not notify it.
        assert fresh.adapters == {} and fresh.workers == {}
        assert importlib.import_module(module.__name__) is not module

        # Positive control ONLY; not part of the automatic recovery claim.
        telegram.rewire_plugin_handlers()
        vk.rewire_plugin_handlers()
        assert fresh.adapters['telegram'] == (native, telegram)
        assert fresh.adapters['vk'] == (None, vk)
        assert len(fresh.workers) == 2 and len(handlers) == 1
        assert not old.live  # The disposed runtime is never revived.
    finally:
        unsubscribe()
        for unsubscribe_rewire in runner._plugin_rewire_unsubscribe.values():
            unsubscribe_rewire()


@pytest.mark.asyncio
@pytest.mark.parametrize('targeted', [False, True], ids=['unload-all', 'targeted-disable'])
async def test_true_unload_terminates_without_resurrection(installed, monkeypatch, targeted):
    manager, _, home = installed
    runner, telegram, vk, native, handlers = adapters(manager, monkeypatch)
    old = runtime(manager)
    await settle()
    manager.unload('deferred-questions' if targeted else None)
    await settle()
    assert not old.live and old.adapters == {} and old.cleanups == []
    assert all(task.done() for task in old.workers.values())
    assert handlers == []
    # Host targeted unload currently leaves factory entries untracked; the
    # disposed runtime's live fence nevertheless prevents those stale factories
    # from starting SDK listeners/workers. Unload-all clears the table outright.
    if not targeted:
        assert manager.get_platform_handler_factories('telegram') == []
        assert manager.get_platform_handler_factories('vk') == []
    # Disabled discovery cannot recreate a runtime or reattach a stale callback.
    home.joinpath('config.yaml').write_text('plugins:\n  enabled: []\n')
    manager.discover_and_load(force=True)
    await settle()
    telegram.rewire_plugin_handlers()
    vk.rewire_plugin_handlers()
    assert not manager._plugins['deferred-questions'].enabled
    assert manager.get_platform_handler_factories('telegram') == []
    assert manager.get_platform_handler_factories('vk') == []
    assert not old.live and old.adapters == {} and handlers == []
    for unsubscribe_rewire in runner._plugin_rewire_unsubscribe.values():
        unsubscribe_rewire()


def test_relative_coordinator_state_is_evicted_with_plugin(installed):
    manager, module, _ = installed
    # Model a future ordinary plugin-owned coordinator import, not a magic
    # sys.modules alias outside the loader's ownership namespace.
    coordinator_name = runtime(manager).__class__.__module__
    coordinator = importlib.import_module(coordinator_name)
    marker = object()
    coordinator.lifecycle_probe = marker
    manager.discover_and_load(force=True)
    replacement = importlib.import_module(coordinator_name)
    assert replacement is not coordinator
    assert not hasattr(replacement, 'lifecycle_probe')
    assert sys.modules[coordinator_name] is replacement


@pytest.mark.asyncio
async def test_targeted_unload_preserves_unrelated_plugin_sdk_handler(installed, monkeypatch):
    manager, _, home = installed
    other = home / 'plugins' / 'unrelated-lifecycle-probe'
    other.mkdir()
    other.joinpath('plugin.yaml').write_text(
        'name: unrelated-lifecycle-probe\nversion: 1.0.0\ndescription: Isolated lifecycle probe\n')
    other.joinpath('__init__.py').write_text(
        'def register(ctx):\n'
        '    def factory(native, adapter):\n'
        '        handler = object()\n'
        '        native.add_handler(handler)\n'
        '        ctx.on_unload(lambda: native.remove_handler(handler))\n'
        "    ctx.register_platform_handler('telegram', factory)\n")
    home.joinpath('config.yaml').write_text(
        'plugins:\n  enabled: [deferred-questions, unrelated-lifecycle-probe]\n')
    manager.discover_and_load(force=True)
    runner, telegram, vk, native, handlers = adapters(manager, monkeypatch)
    await settle()
    old = runtime(manager)
    assert len(handlers) == 2
    unrelated_handler = next(handler for handler in handlers if type(handler) is object)
    manager.unload('deferred-questions')
    await settle()
    assert not old.live and all(task.done() for task in old.workers.values())
    assert handlers == [unrelated_handler]
    telegram.rewire_plugin_handlers()
    vk.rewire_plugin_handlers()
    assert handlers == [unrelated_handler] and old.adapters == {}
    for unsubscribe_rewire in runner._plugin_rewire_unsubscribe.values():
        unsubscribe_rewire()


@pytest.mark.asyncio
async def test_one_profile_force_does_not_dispose_other_manager(installed, monkeypatch, tmp_path):
    from hermes_cli.plugins import PluginManager
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override

    primary, _, _ = installed
    other_home = tmp_path / 'home' / '.hermes' / 'profiles' / 'secondary'
    target = other_home / 'plugins' / 'deferred-questions'
    target.mkdir(parents=True)
    shutil.copy2(PROJECT / 'plugin.yaml', target / 'plugin.yaml')
    for name in ('__init__.py', 'store.py', 'runtime.py', 'lifecycle.py'):
        shutil.copy2(PROJECT / 'hermes_deferred_questions' / name, target / name)
    other_home.joinpath('config.yaml').write_text('plugins:\n  enabled: [deferred-questions]\n')
    secondary = PluginManager(scope_key=str(other_home))
    secondary.discover_and_load()
    token = set_hermes_home_override(other_home)
    runner = None
    try:
        runner, telegram, vk, native, handlers = adapters(secondary, monkeypatch)
        secondary_rt = runtime(secondary)
        await settle()
        assert set(secondary_rt.adapters) == {'telegram', 'vk'}
        primary.discover_and_load(force=True)
        await settle()
        assert runtime(secondary) is secondary_rt and secondary_rt.live
        assert secondary_rt.adapters['telegram'] == (native, telegram)
        assert secondary_rt.adapters['vk'] == (None, vk)
        assert all(not task.done() for task in secondary_rt.workers.values())
        assert len(handlers) == 1
        assert runtime(primary).store.home != secondary_rt.store.home
    finally:
        secondary.unload()
        await settle()
        reset_hermes_home_override(token)
        if runner:
            for unsubscribe_rewire in runner._plugin_rewire_unsubscribe.values():
                unsubscribe_rewire()
