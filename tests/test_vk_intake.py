"""Real VK intake -> base dispatch -> canonical gateway plugin hook. No network."""
import asyncio
import json
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_deferred_questions import request, runtime


async def harness(installed, monkeypatch, peer='2000000123', projects=True, mention=True, native_callbacks=False):
    from gateway.config import PlatformConfig
    from gateway.run_inbound import GatewayInboundMixin
    from plugins.platforms.vk.adapter import VKAdapter
    manager = installed[0]
    rt = runtime(manager)
    extra = {'group_id': '99', 'require_mention': mention,
             'allowed_users': ['456', '789'], 'access_policy': 'user_only'}
    if projects:
        extra['project_lanes'] = {peer: {'lanes': [
            {'id': 'a', 'name': 'Project A'}, {'id': 'b', 'name': 'Project B'}]}}
    adapter = VKAdapter(PlatformConfig(enabled=True, extra=extra))
    if not native_callbacks:
        adapter.register_callback_handler = None  # Explicit legacy-adapter contract.
    adapter._running = True
    adapter.set_authorization_check(lambda uid, *a, **kw: uid in ('456', '789'))
    # Only SDK/API metadata is controlled, never gate, selector, dispatcher or source.
    adapter._enrich_message_from_api = AsyncMock(side_effect=lambda message, **kw: message)
    adapter._resolve_conversation_context = AsyncMock(return_value=('Controlled chat', None))
    calls = []
    async def vk_method(method, params):
        calls.append((method, params))
        if method == 'messages.send':
            return {'response': [{'peer_id': int(peer), 'conversation_message_id': 42}]}
        if method == 'messages.edit':
            return {'response': 1}
        raise AssertionError(f'Unexpected SDK call {method}')
    adapter._vk_method = AsyncMock(side_effect=vk_method)
    seen, forwarded, results = [], [], []
    class Gateway(GatewayInboundMixin):
        def _is_user_authorized_for_source(self, source):
            return source.user_id in ('456', '789')
    gateway = Gateway()
    async def hook(*args, **kwargs):
        outcomes = await manager.ainvoke_hook(*args, **kwargs)
        results.extend(outcomes)
        return outcomes
    monkeypatch.setattr('hermes_cli.lifecycle.ainvoke_hook', hook)
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    async def inbound(event):
        seen.append(event)
        outcome = await gateway._hm_pre_gateway_dispatch_hook(event, event.source)
        if outcome is not None and gateway._is_user_authorized_for_source(outcome.source):
            forwarded.append(outcome)
    adapter.set_message_handler(inbound)
    adapter._wire_plugin_handlers(None)
    # Avoid racing the real periodic worker against assertions; it is plugin-owned.
    for worker in rt.workers.values():
        worker.cancel()
    await asyncio.sleep(0)
    return NS(rt=rt, adapter=adapter, peer=peer, calls=calls, seen=seen,
              forwarded=forwarded, results=results, gateway=gateway)


async def intake(h, text, payload=None, actor='456', reply=None):
    message = {'peer_id': int(h.peer), 'from_id': int(actor), 'text': text,
               'conversation_message_id': 100 + len(h.seen), 'date': int(time.time())}
    if payload is not None:
        message['payload'] = payload
    if reply is not None:
        message['reply_message'] = reply
    await h.adapter._handle_message_update({'type': 'message_new', 'object': {'message': message}})
    # Wait native dispatch tasks, not arbitrary sleeps or a synthetic event.
    while h.adapter._intake_tasks:
        await asyncio.wait_for(asyncio.gather(*list(h.adapter._intake_tasks)), 5)


def new_question(h, thread='lane:a', options=None):
    return h.rt.store.create(request('vk', chat_id=h.peer,
        chat_type='thread' if thread else ('group' if int(h.peer) >= 2000000000 else 'dm'),
        thread_id=thread, options=options or ['Walk', 'Bus', 'Stay home']), h.rt.platforms)


def sent_args(h):
    return [params for method, params in h.calls if method == 'messages.send'][-1]


@pytest.mark.asyncio
@pytest.mark.parametrize('peer,projects,thread', [
    ('2000000123', True, 'lane:a'), ('123', True, 'lane:a'),
    ('2000000123', False, None), ('123', False, None)])
async def test_addressed_buttons_real_invocation_original_lane(installed, monkeypatch, peer, projects, thread):
    h = await harness(installed, monkeypatch, peer, projects)
    row = new_question(h, thread)
    if projects:
        await h.adapter._set_active_lane_id(peer, '456', 'b')
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert 'keyboard' not in args  # old adapter: never emit newly addressed text buttons
    prefix = ('@club99 ' if int(peer) >= 2000000000 else '') + ('@a ' if thread else '')
    action = {'label': prefix + 'Ответ: Bus', 'payload': json.dumps(
        {'hermes_deferred_question': f"dq:{row['id']}:2"})}
    assert args['random_id'] == -row['delivery_nonce']
    await intake(h, action['label'], action['payload'])
    assert len(h.seen) == 1 and not h.forwarded
    assert h.seen[0].source.thread_id == thread
    assert h.seen[0].source.chat_type == row['request']['chat_type']
    assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'
    if projects:
        assert h.adapter._get_active_lane_id(peer, '456') == 'b'


@pytest.mark.asyncio
async def test_legacy_unaddressed_click_never_reaches_hook(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    await intake(h, 'Bus', json.dumps({'hermes_deferred_question': f"dq:{row['id']}:2"}))
    assert not h.seen and not h.rt.store.get(row['id'])['answer']


@pytest.mark.asyncio
@pytest.mark.parametrize('pending', ['pending_edit', 'pending_create'])
async def test_pending_project_capability_and_late_route_rejection(installed, monkeypatch, pending):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    action = {'label': '@club99 @a Ответ: Bus', 'payload': json.dumps(
        {'hermes_deferred_question': f"dq:{row['id']}:2"})}
    h.adapter._lane_state[pending][h.adapter._state_key(h.peer, '456')] = {
        'lane_id': 'b', 'expires_at': time.time() + 3600}
    labels, reason, prefix = h.rt.vk_text_address(h.adapter, row)
    assert not labels and reason == 'pending-project-input' and prefix == '@club99 @a '
    await intake(h, action['label'], action['payload'])
    assert not h.forwarded
    # Pending edit outranks one-shot; plugin refuses to substitute the expected lane.
    if pending == 'pending_edit':
        assert h.seen[0].source.thread_id == 'lane:b'
        assert not h.rt.store.get(row['id'])['answer']
        assert h.results[-1]['reason'] == 'deferred-question:wrong_route'
    else:
        assert h.seen[0].source.thread_id == 'lane:a'
        assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'
    # Native host clears pending state before the plugin hook: documented seam limit.
    assert not h.adapter._lane_state[pending]


@pytest.mark.asyncio
async def test_root_with_active_project_is_explicit_unsupported_fallback(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    await h.adapter._set_active_lane_id(h.peer, '456', 'b')
    row = new_question(h, None)
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert 'keyboard' not in args and 'root-with-projects' in args['message']
    assert 'явный выбор root' in args['message']
    # Even a reply to bot's root prompt does not override active project B.
    await intake(h, f"dq {row['id']} 2", reply={
        'from_id': -99, 'conversation_message_id': 42, 'text': args['message']})
    assert h.seen[0].source.thread_id == 'lane:b'
    assert not h.rt.store.get(row['id'])['answer'] and not h.forwarded


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['budget', 'pending', 'count'])
async def test_fallback_native_text_and_reply_original_lane(installed, monkeypatch, mode):
    h = await harness(installed, monkeypatch)
    options = ['A' * 35, 'Bus'] if mode == 'budget' else (
        [f'Choice {i}' for i in range(11)] if mode == 'count' else ['Walk', 'Bus'])
    row = new_question(h, options=options)
    if mode == 'pending':
        await h.adapter._set_pending_edit(h.peer, '456', 'b')
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert 'keyboard' not in args
    assert f"@club99 @a dq {row['id']} НОМЕР" in args['message']
    await h.adapter._set_active_lane_id(h.peer, '456', 'b')
    if mode == 'pending':
        # A reply to the remembered lane takes precedence over pending edit B.
        await intake(h, f"dq {row['id']} 2", reply={
            'from_id': -99, 'conversation_message_id': 42, 'text': args['message']})
    else:
        await intake(h, f"@club99 @a dq {row['id']} 2")
    assert h.seen[0].source.thread_id == 'lane:a' and not h.forwarded
    assert h.rt.store.get(row['id'])['answer']['value'] == options[1]


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['unknown', 'wrong-actor', 'wrong-route', 'acl-denied', 'gateway-denied'])
async def test_owned_tokens_fail_closed_after_native_intake(installed, monkeypatch, case):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    qid = 'f' * 32 if case == 'unknown' else row['id']
    actor = '789' if case == 'wrong-actor' else ('888' if case == 'acl-denied' else '456')
    if case == 'gateway-denied':
        h.gateway._is_user_authorized_for_source = lambda source: False
    label = '@club99 @b Ответ: Bus' if case == 'wrong-route' else '@club99 @a Ответ: Bus'
    await intake(h, label, json.dumps({'hermes_deferred_question': f'dq:{qid}:2'}), actor)
    assert not h.rt.store.get(row['id'])['answer']
    assert not h.forwarded
    if case not in ('acl-denied', 'gateway-denied'):
        assert len(h.seen) == 1 and h.results[-1]['action'] == 'skip'


@pytest.mark.asyncio
async def test_unrelated_and_quoted_token_preserved(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    await intake(h, '@club99 ordinary message', json.dumps({'other_plugin': 'keep'}))
    await intake(h, '@club99 ordinary reply', reply={
        'from_id': -99, 'conversation_message_id': 42, 'text': 'dq ' + 'f' * 32 + ' 2'})
    assert len(h.forwarded) == 2 and not h.results


@pytest.mark.asyncio
async def test_saved_addressed_button_after_controlled_reload(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    action = {'label': '@club99 @a Ответ: Bus', 'payload': json.dumps(
        {'hermes_deferred_question': f"dq:{row['id']}:2"})}
    installed[0].discover_and_load(force=True)
    new = await harness(installed, monkeypatch)
    assert new.rt is not h.rt and not h.rt.live
    await new.adapter._set_active_lane_id(new.peer, '456', 'b')
    await intake(new, action['label'], action['payload'])
    assert not new.forwarded and new.seen[0].source.thread_id == 'lane:a'
    assert new.rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
@pytest.mark.parametrize('fallback', [False, True])
async def test_network_receipt_survives_local_lane_failure(installed, monkeypatch, fallback, caplog):
    h = await harness(installed, monkeypatch)
    row = new_question(h, options=['A' * 35, 'Bus'] if fallback else None)
    h.adapter._remember_message_lane = AsyncMock(side_effect=OSError('controlled local failure'))
    await h.rt.dispatch_once('vk')
    stored = h.rt.store.get(row['id'])
    assert stored['state'] == 'sent' and stored['message_id'] == 'cmid:42'
    assert 'local lane bookkeeping failed' in caplog.text
    await h.rt.dispatch_once('vk')
    assert len([call for call in h.calls if call[0] == 'messages.send']) == 1
    await intake(h, f"@club99 @a dq {row['id']} 2")
    assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
async def test_receipt_persisted_before_bookkeeping_cancellation(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    started = asyncio.Event()
    async def bookkeeping(*args):
        started.set()
        await asyncio.Event().wait()
    h.adapter._remember_message_lane = bookkeeping
    task = asyncio.create_task(h.rt.dispatch_once('vk'))
    await asyncio.wait_for(started.wait(), 5)
    assert h.rt.store.get(row['id'])['state'] == 'sent'
    assert h.rt.store.get(row['id'])['message_id'] == 'cmid:42'
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await h.rt.dispatch_once('vk')
    assert len([call for call in h.calls if call[0] == 'messages.send']) == 1


@pytest.mark.asyncio
async def test_no_options_text_fallback_uses_real_lane_intake(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    row = h.rt.store.create(request('vk', chat_id=h.peer, chat_type='thread',
        thread_id='lane:a', options=[], allow_text=True), h.rt.platforms)
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert 'keyboard' not in args
    assert f"@club99 @a dq {row['id']} text: ВАШ ОТВЕТ" in args['message']
    assert 'НОМЕР' not in args['message']
    await h.adapter._set_active_lane_id(h.peer, '456', 'b')
    await intake(h, f"@club99 @a dq {row['id']} text: My own route")
    assert not h.forwarded and h.rt.store.get(row['id'])['answer']['value'] == 'My own route'


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['missing-community', 'missing-native-seam'])
async def test_missing_text_address_capabilities(installed, monkeypatch, kind):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    if kind == 'missing-community':
        h.adapter.group_id = ''
    else:
        h.adapter._select_message_lane = None
    labels, reason, _ = h.rt.vk_text_address(h.adapter, row)
    assert not labels and reason in ('community-address-unavailable', 'native-intake-unavailable')
    await h.rt.dispatch_once('vk')
    assert 'keyboard' not in sent_args(h)
    assert reason in sent_args(h)['message']
