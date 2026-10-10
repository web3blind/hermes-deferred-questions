"""Additional actual-adapter and optional SDK-contract tests."""
import asyncio
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
import sys
from pathlib import Path

import pytest
sys.path.insert(0, str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter


@pytest.mark.asyncio
async def test_feishu_native_card_event_to_real_gateway_hook(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    from plugins.platforms.feishu.adapter import FeishuAdapter
    from gateway.config import PlatformConfig
    from gateway.run_inbound import GatewayInboundMixin
    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    adapter._running = True
    adapter.set_authorization_check(lambda uid, *args, **kw: uid == 'ou_456')
    adapter._feishu_send_with_retry = AsyncMock(return_value=NS(success=lambda: True,
                                                               data=NS(message_id='om_42'), code=0))
    adapter._resolve_sender_profile = AsyncMock(return_value={'user_id': 'ou_456', 'user_name': 'User', 'user_id_alt': None})
    adapter.get_chat_info = AsyncMock(return_value={'name': 'test', 'type': 'dm'})
    adapter._resolve_channel_prompt = lambda chat_id: None
    class Gateway(GatewayInboundMixin):
        def _is_user_authorized_for_source(self, source):
            return source.user_id == 'ou_456'
    gateway = Gateway()
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', manager.ainvoke_hook)
    seen = []
    handled = asyncio.Event()
    outcomes = []
    async def inbound(event):
        seen.append(event)
        outcomes.append(await gateway._hm_pre_gateway_dispatch_hook(event, event.source))
        handled.set()
        return None
    adapter.set_message_handler(inbound)
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    row = rt.store.create(request('feishu', user_id='ou_456', chat_id='oc_123', chat_type='group'), rt.platforms)
    adapter._wire_plugin_handlers(None)
    await rt.dispatch_once('feishu')
    assert rt.store.get(row['id'])['state'] == 'sent'
    payload = json.loads(adapter._feishu_send_with_retry.call_args.kwargs['payload'])
    token = payload['elements'][1]['actions'][1]['value']['hermes_deferred_question']
    data = NS(event=NS(token='click-unique', operator=NS(open_id='ou_456'),
                       context=NS(open_chat_id='oc_123', open_message_id='om_42'),
                       action=NS(tag='button', value={'hermes_deferred_question': token})))
    await adapter._handle_card_action_event(data)
    await asyncio.wait_for(handled.wait(), timeout=3)
    assert outcomes == [None]
    assert len(seen) == 1
    assert seen[0].source.chat_id == 'oc_123'
    assert rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
async def test_discord_button_native_event_contract(installed, monkeypatch):
    """No discord.py installed on this host: controlled SDK surface, NOT live SDK E2E."""
    rt = runtime(installed[0])
    class View:
        def __init__(self, timeout): self.children = []; self.stopped = False
        def add_item(self, button): self.children.append(button)
        def stop(self): self.stopped = True
    monkeypatch.setitem(sys.modules, 'discord', NS(ui=NS(View=View, Button=lambda **kw: NS(**kw)),
                                                   AllowedMentions=NS(none=lambda: 'none')))
    listeners = {}
    channel = NS(id=123, guild=NS(id=77), parent_id=None)
    channel.send = AsyncMock(return_value=NS(id=42, channel=channel))
    native = NS(get_channel=lambda cid: channel, fetch_channel=AsyncMock(),
                add_listener=lambda cb, name: listeners.update({name: cb}),
                remove_listener=lambda cb, name: listeners.pop(name, None))
    adapter = ControlledAdapter.make('discord')
    row = rt.store.create(request('discord', chat_type='channel', scope_id='77'), rt.platforms)
    rt.wire('discord', native, adapter)
    await rt.dispatch_once('discord')
    view = channel.send.call_args.kwargs['view']
    assert view.children[1].custom_id == f"dq:{row['id']}:2"
    assert channel.send.call_args.kwargs['allowed_mentions'] == 'none'
    response = NS(is_done=lambda: False, send_message=AsyncMock())
    interaction = NS(data={'custom_id': view.children[1].custom_id}, channel=channel, channel_id=123,
                     guild=channel.guild, user=NS(id=456, bot=False), message=NS(id=42), response=response)
    await listeners['on_interaction'](interaction)
    assert rt.store.get(row['id'])['answer']['value'] == 'Bus'
    assert response.send_message.call_args.kwargs['ephemeral']
    rt.close()
    assert not listeners and view.stopped


@pytest.mark.asyncio
async def test_dispatch_holds_denied_unknown_and_misrouted_profiles(installed):
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('signal')
    row = rt.store.create(request('signal'), rt.platforms)
    rt.wire('signal', None, adapter)
    for value in (False, None, 1):
        adapter.set_authorization_check(lambda *args, **kw: value)
        await rt.dispatch_once('signal')
        assert not adapter.sent
        assert rt.store.get(row['id'])['state'] == 'queued'
    adapter.set_authorization_check(lambda *args, **kw: True)
    original = adapter.build_source
    def misroute(**kwargs):
        source = original(**kwargs); source.profile = 'another-profile'; return source
    adapter.build_source = misroute
    await rt.dispatch_once('signal')
    assert not adapter.sent
    assert rt.store.get(row['id'])['state'] == 'queued'


@pytest.mark.asyncio
async def test_telegram_wrong_callback_message_user_thread_rejected(installed):
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('telegram')
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    token = row['snapshot']['buttons'][0]['token']
    for changes, mid, expected in [({'user_id': 'other'}, '42', 'unauthorized'),
                                   ({'chat_id': 'other'}, '42', 'wrong_route'),
                                   ({'thread_id': '7'}, '42', 'wrong_route'),
                                   ({}, '43', 'wrong_message')]:
        values = {'chat_id': '123', 'user_id': '456', 'chat_type': 'dm', **changes}
        source = adapter.build_source(**values)
        assert rt.callback(adapter, token, source, mid) == expected
    assert rt.store.get(row['id'])['answer'] is None


def test_copied_database_does_not_cross_profile(installed, tmp_path):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    other_home = tmp_path / 'different-profile'
    other = installed[1].store.Store(other_home, 'another')
    # SQLite backup captures WAL, unlike copying only the main DB file.
    with rt.store.connect() as source, other.connect() as dest:
        source.backup(dest)
    assert other.answer(row['id'], '1', row['request'], authorized=True) == 'wrong_profile'
    assert other.get(row['id'])['answer'] is None
