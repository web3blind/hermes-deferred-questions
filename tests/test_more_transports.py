import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
sys.path.insert(0, str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize('options', [['One', 'Two'], ['A', 'B', 'C', 'D', 'E']])
async def test_whatsapp_cloud_native_message_decode_and_buttons(installed, options, monkeypatch):
    from gateway.platforms.whatsapp_cloud import WhatsAppCloudAdapter
    from gateway.config import PlatformConfig
    from gateway.platforms.base import SendResult
    rt = runtime(installed[0])
    adapter = WhatsAppCloudAdapter(PlatformConfig(enabled=True))
    adapter._running = True
    adapter.set_authorization_check(lambda *a, **kw: True)
    adapter._is_dm_allowed = lambda uid: uid == '456'
    adapter._should_process_message = lambda msg: True
    adapter._post_message_result = AsyncMock(return_value=SendResult(success=True, message_id='wamid.42'))
    row = rt.store.create(request('whatsapp_cloud', chat_id='456', options=options), rt.platforms)
    rt.wire('whatsapp_cloud', None, adapter)
    await rt.dispatch_once('whatsapp_cloud')
    payload = adapter._post_message_result.call_args.args[0]
    interactive = payload['interactive']
    if len(options) <= 3:
        token = interactive['action']['buttons'][1]['reply']['id']
        reply_type = 'button_reply'
    else:
        token = interactive['action']['sections'][0]['rows'][1]['id']
        reply_type = 'list_reply'
    raw = {'id': 'wamid.inbound', 'from': '456', 'timestamp': '1', 'type': 'interactive',
           'interactive': {reply_type: {'id': token, 'title': options[1]}}, 'context': {'id': 'wamid.42'}}
    # Actual Cloud adapter's unclaimed-prefix event path, not a synthetic test MessageEvent.
    event = await adapter._build_message_event_from_cloud(raw, {'456': 'User'}, {})
    assert event is not None
    results = await installed[0].ainvoke_hook('pre_gateway_dispatch', event=event,
                                             gateway=NS(_is_user_authorized_for_source=lambda source: True))
    assert results == [{'action': 'skip', 'reason': 'deferred-question:answered'}]
    assert rt.store.get(row['id'])['answer']['value'] == options[1]


@pytest.mark.asyncio
async def test_line_native_quick_reply_and_null_wire_id(installed):
    from plugins.platforms.line.adapter import LineAdapter
    from gateway.config import PlatformConfig
    from gateway.platforms.event import MessageEvent
    rt = runtime(installed[0])
    adapter = LineAdapter(PlatformConfig(enabled=True))
    adapter._running = True
    adapter.set_authorization_check(lambda *a, **kw: True)
    adapter._client = NS(push=AsyncMock(return_value=None))
    row = rt.store.create(request('line'), rt.platforms)
    rt.wire('line', NS(), adapter)
    await rt.dispatch_once('line')
    assert rt.store.get(row['id'])['state'] == 'sent'
    assert rt.store.get(row['id'])['message_id'] is None
    messages = adapter._client.push.call_args.args[1]
    answer = messages[0]['quickReply']['items'][2]['action']['text']
    assert answer == f"dq {row['id']} 3"
    event = MessageEvent(text=answer, source=adapter.build_source(chat_id='123', user_id='456'))
    assert rt.pre_dispatch(event, NS(_is_user_authorized_for_source=lambda source: True))['action'] == 'skip'
    assert rt.store.get(row['id'])['answer']['value'] == 'Stay home'


@pytest.mark.asyncio
async def test_vk_legacy_text_button_intake_remains_durable(installed, monkeypatch):
    from test_vk_intake import harness, intake, new_question, sent_args
    h = await harness(installed, monkeypatch, projects=False)
    row = new_question(h, None)
    await h.rt.dispatch_once('vk')
    assert h.rt.store.get(row['id'])['message_id'] == 'cmid:42'
    params = sent_args(h)
    assert 'keyboard' not in params
    assert params['random_id'] == -row['delivery_nonce']
    # Intake compatibility for a text-action button delivered by older versions.
    await intake(h, '@club99 Ответ: Bus', json.dumps(
        {'hermes_deferred_question': f"dq:{row['id']}:2"}))
    assert not h.forwarded and h.rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
async def test_plain_transport_ack_without_wire_id_and_large_option_fallback(installed):
    from gateway.platforms.base import SendResult
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('matrix')
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id=None))
    row = rt.store.create(request('matrix', options=[f'Choice {n}' for n in range(25)]), rt.platforms)
    rt.wire('matrix', None, adapter)
    await rt.dispatch_once('matrix')
    stored = rt.store.get(row['id'])
    assert stored['state'] == 'sent' and stored['message_id'] is None
    assert '25. Choice 24' in adapter.send.call_args.args[1]
    assert rt.store.answer(row['id'], '25', row['request'], authorized=True) == 'answered'


@pytest.mark.asyncio
async def test_two_dispatchers_and_concurrent_answers_only_commit_once(installed):
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('signal')
    row = rt.store.create(request('signal'), rt.platforms)
    rt.wire('signal', None, adapter)
    await asyncio.gather(rt.dispatch_once('signal'), rt.dispatch_once('signal'))
    assert len(adapter.sent) == 1
    results = await asyncio.gather(*[asyncio.to_thread(rt.store.answer, row['id'], str(i), row['request'], authorized=True)
                                    for i in (1, 2)])
    assert sorted(results) == ['already_answered', 'answered']
