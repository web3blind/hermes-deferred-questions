"""Private snapshots through actual factory, worker and lifecycle paths."""
import asyncio
import json
import os
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_deferred_questions import ControlledAdapter, cli, request, runtime


def native():
    return NS(bot=NS(send_message=AsyncMock(return_value=NS(
                  message_id=321, chat=NS(id=123), message_thread_id=None))),
              add_handler=lambda handler, group: None,
              remove_handler=lambda handler, group: None)


def snapshot(home):
    path = home / 'deferred-questions' / 'telegram-runtime.json'
    assert path.stat().st_mode & 0o777 == 0o600
    data = json.loads(path.read_text())
    assert set(data) == {'pid', 'generation', 'timestamp', 'live', 'bound',
                         'worker_alive', 'adapter_is_connected', 'reason', 'error_stage'}
    assert data['pid'] == os.getpid()
    return data


def wire(manager, adapter, client=None):
    factory = manager.get_platform_handler_factories('telegram')[0][0]
    factory(client or native(), adapter)
    return factory


async def wait_snapshot(home, predicate):
    async def poll():
        while True:
            value = snapshot(home)
            if predicate(value):
                return value
            await asyncio.sleep(.01)
    return await asyncio.wait_for(poll(), 4)


@pytest.mark.asyncio
async def test_other_platform_does_not_create_snapshot(installed):
    manager, module, home = installed
    factory = manager.get_platform_handler_factories('signal')[0][0]
    factory(None, ControlledAdapter.make('signal'))
    await asyncio.sleep(.05)
    assert not (home / 'deferred-questions' / 'telegram-runtime.json').exists()
    manager.unload()
    await asyncio.sleep(.05)


@pytest.mark.asyncio
async def test_worker_completion_is_observed(installed):
    manager, module, home = installed
    rt = runtime(manager)
    wire(manager, ControlledAdapter.make('telegram'))
    task = rt.workers['telegram']
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    data = await wait_snapshot(home, lambda s: not s['worker_alive'])
    assert data['bound'] and data['live']


@pytest.mark.asyncio
async def test_same_runtime_adapter_replacement_is_observed(installed):
    manager, module, home = installed
    rt = runtime(manager)
    first = ControlledAdapter.make('telegram')
    wire(manager, first)
    old_worker = rt.workers['telegram']
    second = ControlledAdapter.make('telegram')
    wire(manager, second)
    await wait_snapshot(home, lambda s: s['bound'] and s['worker_alive'] and s['reason'] is None)
    assert rt.adapters['telegram'][1] is second
    assert old_worker.done() and rt.workers['telegram'] is not old_worker
    assert snapshot(home)['generation'] == rt.generation


@pytest.mark.asyncio
async def test_worker_stage_failure_is_fixed_and_redacted(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    async def fail(platform):
        raise RuntimeError('PRIVATE_STAGE_EXCEPTION')
    monkeypatch.setattr(rt, 'dispatch_once', fail)
    wire(manager, ControlledAdapter.make('telegram'))
    data = await wait_snapshot(home, lambda s: s['reason'] == 'dispatch_stage_failed')
    assert data['error_stage'] == 'dispatch' and data['worker_alive']
    assert 'PRIVATE_STAGE_EXCEPTION' not in json.dumps(data)


def test_cli_only_does_not_create_snapshot(installed, capsys):
    manager, module, home = installed
    cli(manager, 'list')
    assert not (home / 'deferred-questions' / 'telegram-runtime.json').exists()
    manager.unload()
    assert not (home / 'deferred-questions' / 'telegram-runtime.json').exists()


@pytest.mark.asyncio
async def test_connected_notready_cli_and_unload(installed, capsys):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('telegram')
    factory = wire(manager, adapter)
    first = snapshot(home)
    assert rt.generation in factory.__qualname__
    assert first['generation'] == rt.generation
    assert first['bound'] and first['worker_alive'] and first['adapter_is_connected']
    assert first['reason'] is None and first['error_stage'] is None
    # A separate CLI-only Runtime with the same Store must not clobber it.
    from hermes_plugins.deferred_questions.runtime import Runtime
    other = Runtime(NS(on_unload=lambda fn: None), rt.store, rt.platforms)
    other.close()
    cli(manager, 'list')
    assert snapshot(home) == first
    adapter._running = False
    rt._heartbeat()  # Snapshot on an event, not a periodic idle heartbeat.
    later = await wait_snapshot(home, lambda s: s['reason'] == 'not_connected')
    assert later['timestamp'] >= first['timestamp']
    assert later['bound'] and later['worker_alive'] and not later['adapter_is_connected']
    adapter._running = True
    rt._heartbeat()
    await wait_snapshot(home, lambda s: s['reason'] is None)
    manager.unload()
    unloaded = snapshot(home)
    assert not unloaded['live'] and not unloaded['bound']
    assert unloaded['reason'] == 'unloaded' and unloaded['error_stage'] == 'lifecycle'
    await asyncio.sleep(.05)
    assert snapshot(home) == unloaded


@pytest.mark.asyncio
@pytest.mark.parametrize('denial', ['authorization_denied', 'route_denied'])
async def test_denied_real_worker_keeps_queue_and_redacts(installed, monkeypatch, denial):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('telegram')
    row = rt.store.create(request(question='PRIVATE_QUESTION', user_id='456'), rt.platforms)
    if denial == 'authorization_denied':
        adapter.set_authorization_check(lambda *args, **kwargs: False)
    else:
        original = adapter.build_source
        def wrong_source(**kwargs):
            source = original(**kwargs)
            source.chat_id = '999'
            return source
        monkeypatch.setattr(adapter, 'build_source', wrong_source)
    client = native()
    wire(manager, adapter, client)
    state = await wait_snapshot(home, lambda s: s['reason'] == denial)
    assert state['error_stage'] == 'dispatch'
    assert rt.store.get(row['id'])['state'] == 'queued'
    client.bot.send_message.assert_not_awaited()
    text = (home / 'deferred-questions' / 'telegram-runtime.json').read_text()
    assert 'PRIVATE_QUESTION' not in text and row['id'] not in text


@pytest.mark.asyncio
async def test_initial_wire_failure_without_worker(installed):
    manager, module, home = installed
    rt = runtime(manager)
    client = native()
    def fail(*args, **kwargs):
        raise RuntimeError('PRIVATE_TOKEN_EXCEPTION')
    client.add_handler = fail
    with pytest.raises(RuntimeError, match='PRIVATE_TOKEN_EXCEPTION'):
        wire(manager, ControlledAdapter.make('telegram'), client)
    data = snapshot(home)
    assert not data['bound'] and not data['worker_alive']
    assert data['reason'] == 'wire_failed' and data['error_stage'] == 'wire'
    assert 'PRIVATE_TOKEN_EXCEPTION' not in json.dumps(data)
    assert 'telegram' not in rt.workers


@pytest.mark.asyncio
async def test_wrong_profile_wire_is_observed_without_binding(installed):
    manager, module, home = installed
    adapter = ControlledAdapter.make('telegram')
    adapter._owner_profile = 'another-profile'
    wire(manager, adapter)
    data = snapshot(home)
    assert data['reason'] == 'route_denied' and data['error_stage'] == 'wire'
    assert not data['bound'] and not data['worker_alive']


@pytest.mark.asyncio
async def test_replaced_generation_old_tasks_do_not_overwrite(installed, monkeypatch):
    manager, module, home = installed
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    adapter = ControlledAdapter.make('telegram')
    client = native()
    adapter._wire_plugin_handlers(client)
    old = runtime(manager)
    old_generation = snapshot(home)['generation']
    manager.discover_and_load(force=True)
    await asyncio.sleep(0)
    fresh = runtime(manager)
    adapter.rewire_plugin_handlers()
    data = await wait_snapshot(home, lambda s: s['generation'] == fresh.generation and s['bound'])
    assert not old.live and data['generation'] != old_generation
    assert data['worker_alive']
    old.close()
    await asyncio.sleep(.05)
    assert snapshot(home)['generation'] == fresh.generation


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['mkstemp', 'replace'])
async def test_write_failure_does_not_change_delivery(installed, monkeypatch, failure):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('telegram')
    client = native()
    row = rt.store.create(request(), rt.platforms)
    import hermes_plugins.deferred_questions.runtime as implementation
    target = implementation.tempfile if failure == 'mkstemp' else implementation.os
    original = getattr(target, failure)
    def fail(*args, **kwargs):
        # Inject a diagnostics-only failure, not failure of required IPC state.
        heartbeat = (str(args[1]).endswith('telegram-runtime.json') if failure == 'replace'
                     else kwargs.get('prefix', '').startswith('.telegram-runtime-'))
        if heartbeat:
            raise OSError('PRIVATE_FILESYSTEM_EXCEPTION')
        return original(*args, **kwargs)
    monkeypatch.setattr(target, failure, fail)
    wire(manager, adapter, client)
    async def delivered():
        while (await asyncio.to_thread(rt.store.get, row['id']))['state'] in ('queued', 'sending'):
            await asyncio.sleep(.01)
    await asyncio.wait_for(delivered(), 4)
    manager.unload()
    await asyncio.sleep(.05)
    assert rt.store.get(row['id'])['state'] == 'sent'
    client.bot.send_message.assert_awaited_once()
    assert not (home / 'deferred-questions' / 'telegram-runtime.json').exists()
    assert not list((home / 'deferred-questions').glob('.telegram-runtime-*'))


@pytest.mark.asyncio
async def test_atomic_replace_retains_previous_snapshot_on_failure(installed, monkeypatch):
    manager, module, home = installed
    adapter = ControlledAdapter.make('telegram')
    client = native()
    wire(manager, adapter, client)
    before = snapshot(home)
    import hermes_plugins.deferred_questions.runtime as implementation
    def fail(*args, **kwargs):
        raise OSError('write failed')
    monkeypatch.setattr(implementation.os, 'replace', fail)
    wire(manager, adapter, client)
    assert snapshot(home) == before
    assert not list((home / 'deferred-questions').glob('.telegram-runtime-*'))
