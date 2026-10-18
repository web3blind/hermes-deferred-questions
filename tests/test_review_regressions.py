"""Review blockers exercised against real Telegram/PTB and GatewayRunner seams."""
import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import pytest
from test_deferred_questions import request, runtime


def telegram(extra=None):
    from gateway.config import PlatformConfig
    from plugins.platforms.telegram.adapter import TelegramAdapter
    adapter = TelegramAdapter(PlatformConfig(enabled=True, extra=extra or {}))
    adapter._running = True
    adapter.set_authorization_check(lambda *args, **kwargs: True)
    return adapter


def message(bot, *, forum=False, thread=7, text='answer', mid=42):
    from telegram import Message
    return Message.de_json({'message_id': mid, 'date': 1, 'text': text,
                            'from': {'id': 456, 'first_name': 'User', 'is_bot': False},
                            'chat': {'id': -123 if forum else 123,
                                     'type': 'supergroup' if forum else 'private', 'is_forum': forum},
                            'is_topic_message': True, 'message_thread_id': thread}, bot)


@pytest.mark.asyncio
async def test_forum_real_decode_text_and_old_button_after_restart(installed, monkeypatch):
    from telegram import Bot, Update
    from telegram.ext import Application
    from gateway.platforms.event import MessageEvent
    from hermes_cli.plugins import PluginManager
    manager, module, home = installed
    rt = runtime(manager)
    adapter = telegram()
    bot = Bot('123456:ISOLATED-TEST-TOKEN')
    app = Application.builder().bot(bot).build()
    initial_scan = asyncio.Event()
    original_queued = rt.store.queued
    def queued(platform):
        result = original_queued(platform)
        initial_scan.set()
        return result
    monkeypatch.setattr(rt.store, 'queued', queued)
    rt.wire('telegram', app, adapter)
    await asyncio.wait_for(initial_scan.wait(), 1)
    await asyncio.gather(*rt.feedback_workers.values())  # Truly idle before manual dispatch.
    text_row = rt.store.create(request(chat_id='-123', chat_type='forum', thread_id='7'), rt.platforms)
    button_row = rt.store.create(request(chat_id='-123', chat_type='forum', thread_id='7',
                                         idempotency_key='button'), rt.platforms)
    with patch.object(Bot, 'send_message', new=AsyncMock(
            side_effect=[message(bot, forum=True), message(bot, forum=True)])) as send:
        await rt.dispatch_once('telegram')
        assert all(call.kwargs['message_thread_id'] == 7 for call in send.call_args_list)
    assert rt.store.get(button_row['id'])['state'] == 'sent'
    gateway = NS(_is_user_authorized_for_source=lambda source: True)
    for thread in (8, 7):
        decoded = message(bot, forum=True, thread=thread, text=f"dq {text_row['id']} 1")
        source = adapter._source_from_message_for_auth(decoded)
        assert source.chat_type == 'forum'
        event = MessageEvent(text=decoded.text, source=source, raw_message=decoded)
        outcome = rt.pre_dispatch(event, gateway)
        assert (outcome is not None) == (thread == 7)
    assert rt.store.get(text_row['id'])['answer']['value'] == 'Walk'
    manager.unload('deferred-questions')
    await asyncio.sleep(0)
    second = PluginManager(scope_key=str(home)); second.discover_and_load()
    try:
        rt2 = runtime(second)
        app2 = Application.builder().bot(bot).build()
        rt2.wire('telegram', app2, adapter)
        handler = app2.handlers[-20][0]
        for thread in (8, 7):
            decoded = message(bot, forum=True, thread=thread)
            payload = decoded.to_dict()
            payload['from'] = {'id': 999, 'first_name': 'Bot', 'is_bot': True}
            update = Update.de_json({'update_id': thread, 'callback_query': {
                'id': f'cb{thread}', 'chat_instance': 'test',
                'from': {'id': 456, 'first_name': 'User', 'is_bot': False},
                'message': payload, 'data': f"dq:{button_row['id']}:2"}}, bot)
            with patch.object(Bot, 'answer_callback_query', new=AsyncMock()) as ack:
                await handler.handle_update(update, app2, handler.check_update(update), NS())
                assert ack.call_args.kwargs['text'] == ('wrong_route' if thread == 8 else 'Answer saved')
        assert rt2.store.get(button_row['id'])['answer']['value'] == 'Bus'
    finally:
        second.unload(); await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong_lane', [None, 'native_only', 'both'])
async def test_private_topic_matches_core_send_and_rejects_native_receipt(installed, wrong_lane):
    from telegram import Bot
    from telegram.ext import Application
    rt = runtime(installed[0]); adapter = telegram()
    bot = Bot('123456:ISOLATED-TEST-TOKEN')
    rt.wire('telegram', Application.builder().bot(bot).build(), adapter)
    await asyncio.sleep(0)
    row = rt.store.create(request(thread_id='7'), rt.platforms)
    metadata = row['snapshot']['metadata']
    assert 'direct_messages_topic_id' not in metadata
    expected = adapter._thread_kwargs_for_send('123', '7', metadata)
    assert expected == {'message_thread_id': 7}
    receipt = NS(message_id=42, chat=NS(id=123), message_thread_id=None if wrong_lane == 'native_only' else 7,
                 direct_messages_topic=NS(topic_id=7) if wrong_lane else None)
    with patch.object(Bot, 'send_message', new=AsyncMock(return_value=receipt)) as send:
        await rt.dispatch_once('telegram')
        assert send.call_args.kwargs['message_thread_id'] == expected['message_thread_id']
        assert 'direct_messages_topic_id' not in send.call_args.kwargs
    assert rt.store.get(row['id'])['state'] == ('unknown' if wrong_lane else 'sent')


@pytest.mark.asyncio
@pytest.mark.parametrize('behavior', ['ignore', 'pair', 'decline'])
@pytest.mark.parametrize('chat_type,extra', [
    ('dm', {'allow_from': ['999']}),
    ('forum', {'groups': {'-123': {'allow_from': ['999']}}, 'group_allow_from': ['456']}),
    ('forum', {'groups': {'*': {'allowFrom': ['999']}}}),
    ('group', {'group_allow_from': ['999']}),
])
async def test_local_restrictions_override_real_runner_grant(installed, monkeypatch, chat_type, extra, behavior):
    from gateway.run import GatewayRunner
    from gateway.config import GatewayConfig, Platform
    from telegram import Bot
    rt = runtime(installed[0]); adapter = telegram({**extra, 'unauthorized_dm_behavior': behavior})
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = GatewayConfig(); runner.adapters = {Platform.TELEGRAM: adapter}
    monkeypatch.setenv('TELEGRAM_ALLOWED_USERS', '456')
    adapter.set_authorization_check(runner._make_adapter_auth_check(Platform.TELEGRAM))
    chat_id = '123' if chat_type == 'dm' else '-123'
    thread_id = '7' if chat_type == 'forum' else None
    source = adapter.build_source(chat_id=chat_id, user_id='456', chat_type=chat_type, thread_id=thread_id)
    decoded = message(Bot('123456:ISOLATED-TEST-TOKEN'), forum=chat_type == 'forum')
    if chat_type == 'group':
        decoded = NS(chat=NS(id=-123, type='supergroup', is_forum=False), from_user=NS(id=456, is_bot=False),
                     message_thread_id=None, is_topic_message=False)
    assert adapter._is_sender_authorized('456', chat_type, chat_id, thread_id=thread_id) is True
    # Native intake rules differ between supported SDK releases; the plugin
    # must apply the more restrictive local policy regardless of that grant.
    assert rt.authorized(adapter, source) is False
    row = rt.store.create(request(chat_id=chat_id, chat_type=chat_type, thread_id=thread_id), rt.platforms)
    rt.adapters['telegram'] = (NS(bot=NS(send_message=AsyncMock())), adapter)
    await rt.dispatch_once('telegram')
    assert rt.store.get(row['id'])['state'] == 'queued'
    assert not rt.adapters['telegram'][0].bot.send_message.called
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    token = row['snapshot']['buttons'][0]['token']
    assert rt.callback(adapter, token, source, '42') == 'unauthorized'
    adapter.config.extra = {'allow_from': ['456'], 'group_allow_from': ['456']}
    assert rt.authorized(adapter, source) is True
    for checker in (None, lambda *args, **kwargs: False, lambda *args, **kwargs: None, lambda *args, **kwargs: 1):
        adapter.set_authorization_check(checker)
        assert rt.authorized(adapter, source) is False
    adapter.set_authorization_check(runner._make_adapter_auth_check(Platform.TELEGRAM))
    assert rt.callback(adapter, token, source, '42') == 'answered'
    allowed = rt.store.create(request(chat_id=chat_id, chat_type=chat_type, thread_id=thread_id,
                                      idempotency_key='allowed-send'), rt.platforms)
    rt.adapters['telegram'][0].bot.send_message.return_value = NS(
        message_id=43, chat=NS(id=int(chat_id)),
        message_thread_id=int(thread_id) if thread_id else None)
    await rt.dispatch_once('telegram')
    assert rt.store.get(allowed['id'])['state'] == 'sent'


@pytest.mark.parametrize('verdict', [None, False, 1, 'exception', 'absent'])
def test_telegram_local_seam_unknown_fails_closed(installed, verdict):
    rt = runtime(installed[0]); adapter = telegram()
    source = adapter.build_source(chat_id='123', user_id='456', chat_type='dm')
    def check(message):
        if verdict == 'exception':
            raise RuntimeError('controlled local policy failure')
        return verdict
    adapter._is_user_authorized_from_message = None if verdict == 'absent' else check
    assert rt.authorized(adapter, source) is False
