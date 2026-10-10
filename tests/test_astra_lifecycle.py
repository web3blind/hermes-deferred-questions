"""Lifecycle counterexamples through host loader/native adapters; no network effects."""
import asyncio

import pytest

from test_deferred_questions import runtime
from test_entrypoint_lifecycle import adapter_for


async def eventually(predicate):
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(.01)
    assert predicate()


@pytest.mark.asyncio
@pytest.mark.parametrize('force', [None, 'before-C', 'after-C'])
@pytest.mark.parametrize('failure', [None, 'timeout', 'cleanup'])
async def test_three_native_replacements_preserve_platform_drain(ep, monkeypatch, force, failure):
    manager, module, _ = ep
    rt = runtime(manager)
    started, finishing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    a, an = adapter_for('telegram')
    b, bn = adapter_for('telegram')
    c, cn = adapter_for('telegram')
    active = []

    async def worker(self, platform):
        adapter = self.adapters[platform][1]
        active.append(adapter)
        if adapter is a:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                finishing.set()
                # Deliberately resistant even to a second cancellation on reload.
                while not release.is_set():
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        pass
        else:
            await asyncio.Event().wait()

    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    if failure == 'timeout':
        monkeypatch.setattr(module.lifecycle, 'DRAIN_TIMEOUT', .08)
    a._wire_plugin_handlers(an)
    await asyncio.wait_for(started.wait(), 2)
    if failure == 'cleanup':
        def broken_cleanup():
            raise RuntimeError('controlled cleanup failure')
        rt._bindings['telegram'][3].append(broken_cleanup)
    try:
        b._wire_plugin_handlers(bn)
        await asyncio.wait_for(finishing.wait(), 2)
        if force == 'before-C':
            manager.discover_and_load(force=True)
            rt = runtime(manager)
        c._wire_plugin_handlers(cn)
        if force == 'after-C':
            manager.discover_and_load(force=True)
            rt = runtime(manager)
        await asyncio.sleep(.04)
        assert 'telegram' not in rt.adapters
        assert not bn.handlers.get(-20) and not cn.handlers.get(-20)
        assert active == [a]
        if failure == 'timeout':
            await eventually(lambda: rt.health()['telegram']['reason'] == 'drain_failed')
        release.set()
        if failure:
            await eventually(lambda: rt.health()['telegram']['reason'] == 'drain_failed')
            # Transfer the already failed receipt, then try a NEW native identity.
            manager.discover_and_load(force=True)
            rt = runtime(manager)
            d, dn = adapter_for('telegram')
            d._wire_plugin_handlers(dn)
            await eventually(lambda: rt.health()['telegram']['reason'] == 'drain_failed')
            await asyncio.sleep(.04)
            assert 'telegram' not in rt.adapters
            assert not bn.handlers.get(-20) and not cn.handlers.get(-20) and not dn.handlers.get(-20)
            assert active == [a]
        else:
            await eventually(lambda: 'telegram' in rt.adapters)
            assert rt.adapters['telegram'] == (cn, c)
            await eventually(lambda: c in active)
            assert active == [a, c]
            assert not bn.handlers.get(-20)
            assert len(cn.handlers[-20]) == 1
    finally:
        release.set()
        manager.unload()
        await asyncio.sleep(.05)


@pytest.mark.asyncio
@pytest.mark.parametrize('stop', ['unload', 'replace'])
@pytest.mark.parametrize('buttons', [False, True])
@pytest.mark.parametrize('phase', ['lookup', 'started-send'])
async def test_discord_stop_fences_lookup_but_preserves_started_send_ack(ep, monkeypatch, stop, buttons, phase):
    import sys
    from types import SimpleNamespace as NS
    from unittest.mock import AsyncMock
    from gateway.config import PlatformConfig
    from plugins.platforms.discord.adapter import DiscordAdapter
    from test_deferred_questions import request

    manager, module, _ = ep
    rt = runtime(manager)
    # Optional SDK boundary only: real host adapter, loader, owned delivery/drain.
    views = []
    class View:
        def __init__(self, timeout):
            views.append(self)
        def add_item(self, button):
            pass
        def stop(self):
            pass
    monkeypatch.setitem(sys.modules, 'discord', NS(
        ui=NS(View=View, Button=lambda **kw: NS(**kw)),
        AllowedMentions=NS(none=lambda: None)))
    started, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    channel = NS(id=123, guild=NS(id=77))
    async def resistant_operation():
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
    async def fetch(target):
        if phase == 'lookup':
            await resistant_operation()
        return channel
    async def send(**kwargs):
        if phase == 'started-send':
            await resistant_operation()
        return NS(id=42, channel=channel)
    channel.send = AsyncMock(side_effect=send)
    native = NS(get_channel=lambda target: None, fetch_channel=fetch,
                add_listener=lambda *a: None, remove_listener=lambda *a: None)
    adapter = DiscordAdapter(PlatformConfig(enabled=True))
    adapter._running = True
    adapter.set_authorization_check(lambda *a, **kw: True)
    async def idle(self, platform):
        await asyncio.Event().wait()
    monkeypatch.setattr(module.runtime.Runtime, 'worker', idle)
    row = rt.store.create(request('discord', scope_id='77', options=['Yes'] if buttons else [],
                                  allow_text=True), rt.platforms)
    adapter._wire_plugin_handlers(native)
    dispatch = asyncio.create_task(rt.dispatch_once('discord'))
    try:
        await asyncio.wait_for(started.wait(), 2)
        if stop == 'unload':
            manager.unload()
        else:
            replacement = DiscordAdapter(PlatformConfig(enabled=True))
            replacement._running = True
            replacement._wire_plugin_handlers(native)
        await asyncio.wait_for(cancelled.wait(), 2)
        release.set()
        await asyncio.wait_for(dispatch, 2)
        expected_sends = int(phase == 'started-send')
        assert channel.send.await_count == expected_sends
        assert len(views) == int(buttons and phase == 'started-send')
        saved = rt.store.get(row['id'])
        if phase == 'started-send':
            assert saved['state'] == 'sent' and saved['message_id'] == '42'
        else:
            assert saved['state'] in ('sending', 'unknown')
        if stop == 'replace':
            await eventually(lambda: rt.adapters.get('discord', (None, None))[1] is replacement)
            await rt.dispatch_once('discord')
            assert channel.send.await_count == expected_sends  # Never blindly resend.
    finally:
        release.set()
        manager.unload()
        await asyncio.sleep(.05)
