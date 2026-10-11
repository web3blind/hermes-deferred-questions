"""Generic registrar consumer contract; real VK routing, controlled SDK, no network."""
import asyncio
import json

import pytest

from test_deferred_questions import request
from test_vk_intake import harness as vk_harness, new_question, sent_args


async def harness(installed, monkeypatch, **kwargs):
    return await vk_harness(installed, monkeypatch, native_callbacks=True, **kwargs)


@pytest.fixture
def registrar(monkeypatch):
    from plugins.platforms.vk.adapter import VKAdapter
    def register(self, namespace, handler):
        handlers = getattr(self, 'test_callbacks', {})
        self.test_callbacks = handlers
        if namespace in handlers:
            raise ValueError('duplicate namespace')
        handlers[namespace] = handler
        def cleanup():
            if handlers.get(namespace) is handler:
                handlers.pop(namespace)
        return cleanup
    monkeypatch.setattr(VKAdapter, 'register_callback_handler', register, raising=False)


def event(row, peer='2000000123', actor=456, cmid=42):
    return {'peer_id': int(peer), 'user_id': actor, 'event_id': 'native-event',
            'conversation_message_id': cmid,
            'payload': {'hermes_callback': 'deferred_questions',
                        'data': {'token': f"dq:{row['id']}:2"}}}


async def click(h, obj):
    return await h.adapter.test_callbacks['deferred_questions'](obj)


@pytest.mark.asyncio
async def test_original_label_callback_receipt_lane_feedback(installed, monkeypatch, registrar):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.adapter._set_active_lane_id(h.peer, '456', 'b')
    await h.adapter._set_pending_edit(h.peer, '456', 'b')
    await h.rt.dispatch_once('vk')
    action = json.loads(sent_args(h)['keyboard'])['buttons'][0][1]['action']
    assert action == {'type': 'callback', 'label': 'Bus', 'payload': json.dumps(
        {'hermes_callback': 'deferred_questions', 'data': {'token': f"dq:{row['id']}:2"}})}
    assert await click(h, event(row)) == 'Answer saved'
    assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'
    assert h.adapter._get_active_lane_id(h.peer, '456') == 'b'
    assert h.adapter._pending_edit(h.peer, '456', prune=False)
    await asyncio.gather(*[task for task in h.rt._tasks['vk'] if not task.done()])
    await h.rt.dispatch_feedback_once('vk')
    assert h.rt.store.feedback(row['id'])['state'] == 'done'
    assert not h.seen and not h.forwarded


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['actor', 'peer', 'cmid', 'missing-cmid', 'missing-lane',
                                  'wrong-lane', 'acl', 'profile', 'payload', 'data-conflict',
                                  'bool-id', 'namespace', 'scope', 'unknown'])
async def test_callback_fences(installed, monkeypatch, registrar, case):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    obj = event(row)
    if case == 'actor':
        obj['user_id'] = 789
    if case == 'peer':
        obj['peer_id'] = 2000000999
    if case == 'cmid':
        obj['conversation_message_id'] = 43
    if case == 'missing-cmid':
        obj.pop('conversation_message_id')
    if case == 'missing-lane':
        h.adapter._lane_state['message_lanes'].clear()
    if case == 'wrong-lane':
        await h.adapter._remember_message_lane(h.peer, 'cmid:42', 'lane:b')
    if case == 'acl':
        h.adapter.set_authorization_check(lambda *a, **kw: False)
    if case == 'profile':
        h.adapter._owner_profile = 'other'
    if case == 'payload':
        obj['payload']['data']['token'] = 'dq:invalid:2'
    if case == 'data-conflict':
        obj['data'] = {'token': f"dq:{row['id']}:1"}
    if case == 'bool-id':
        obj['user_id'] = True
    if case == 'namespace':
        obj['payload']['hermes_callback'] = 'other'
    if case == 'scope':
        build = h.adapter.build_source
        def scoped(**kwargs):
            source = build(**kwargs)
            source.scope_id = 'unexpected'
            return source
        monkeypatch.setattr(h.adapter, 'build_source', scoped)
    if case == 'unknown':
        obj['payload']['data']['token'] = 'dq:' + 'f' * 32 + ':2'
    assert await click(h, obj) != 'Answer saved'
    assert not h.rt.store.get(row['id'])['answer']
    assert not h.rt.store.feedback(row['id'])
    assert not h.seen


@pytest.mark.asyncio
async def test_old_adapter_no_keyboard_explicit_text_only(installed, monkeypatch):
    h = await harness(installed, monkeypatch)
    h.adapter.register_callback_handler = None
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert 'keyboard' not in args
    assert 'callback-registration-unavailable' in args['message']
    assert f"@club99 @a dq {row['id']} NUMBER" in args['message']


@pytest.mark.asyncio
async def test_owned_callback_cleanup_and_saved_button_reload(installed, monkeypatch, registrar):
    h = await harness(installed, monkeypatch)
    row = new_question(h, options=['Пешком', 'Автобус', 'Остаться дома'])
    await h.rt.dispatch_once('vk')
    # A previously sent Russian label remains data, not an English-only key.
    action = json.loads(sent_args(h)['keyboard'])['buttons'][0][1]['action']
    assert action['label'] == 'Автобус'
    saved = event(row)
    old = h.adapter.test_callbacks['deferred_questions']
    h.rt.close()
    await asyncio.sleep(.05)
    assert not h.adapter.test_callbacks
    assert await old(saved) is None
    installed[0].discover_and_load(force=True)
    new = await harness(installed, monkeypatch)
    # Real persisted mapping is loaded into the new adapter, not payload/ledger lane.
    assert await click(new, saved) == 'Answer saved'
    assert new.rt.store.get(row['id'])['answer']['value'] == 'Автобус'


@pytest.mark.asyncio
@pytest.mark.parametrize('peer,thread', [('123', None), ('2000000123', None),
                                        ('123', 'lane:a'), ('2000000123', 'lane:a')])
async def test_native_routes_and_normalized_data(installed, monkeypatch, registrar, peer, thread):
    h = await harness(installed, monkeypatch, peer=peer)
    row = new_question(h, thread)
    await h.adapter._set_active_lane_id(peer, '456', 'b')
    await h.rt.dispatch_once('vk')
    assert 'keyboard' in sent_args(h)
    obj = event(row, peer=peer)
    obj['data'] = obj.pop('payload')['data']
    assert await click(h, obj) == 'Answer saved'
    assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
@pytest.mark.parametrize('options,keyboard', [(['X' * 40], True), (['😀' * 20], True),
    (['X' * 41], False), (['😀' * 21], False), ([str(i) for i in range(11)], False)])
async def test_callback_original_label_budget(installed, monkeypatch, registrar, options, keyboard):
    h = await harness(installed, monkeypatch)
    new_question(h, options=options)
    await h.rt.dispatch_once('vk')
    args = sent_args(h)
    assert ('keyboard' in args) is keyboard
    if keyboard:
        assert json.loads(args['keyboard'])['buttons'][0][0]['action']['label'] == options[0]


@pytest.mark.asyncio
async def test_continuation_and_failed_feedback_survive_reopen(installed, monkeypatch, registrar):
    h = await harness(installed, monkeypatch)
    row = h.rt.store.create(request('vk', chat_id=h.peer, chat_type='thread',
        thread_id='lane:a', continue_agent=True), h.rt.platforms)
    await h.rt.dispatch_once('vk')
    method = h.adapter._vk_method
    async def failed_edit(name, params):
        if name == 'messages.edit':
            raise ConnectionError('controlled')
        return await method(name, params)
    h.adapter._vk_method = failed_edit
    assert await click(h, event(row)) == 'Answer saved'
    await asyncio.gather(*[task for task in h.rt._tasks['vk'] if not task.done()])
    original = h.rt.store
    h.rt.store = type(original)(original.home, original.profile)
    assert h.rt.store.feedback(row['id'])['state'] == 'pending'
    assert len(h.rt.store.pending_actions('vk')) == 1
    await h.rt.dispatch_answers_once('vk')
    while h.adapter._intake_tasks:
        await asyncio.wait_for(asyncio.gather(*list(h.adapter._intake_tasks)), 5)
    await h.rt.dispatch_answers_once('vk')
    assert len(h.seen) == 1
    assert h.seen[0].internal and h.seen[0].source.thread_id == 'lane:a'
    assert h.rt.store.pending_actions('vk') == []
    assert await click(h, event(row)) == 'Answer saved'
    assert h.rt.store.pending_actions('vk') == []


@pytest.mark.asyncio
async def test_owned_callback_cancelled_on_unload(installed, monkeypatch, registrar):
    h = await harness(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    started, finalized = asyncio.Event(), asyncio.Event()
    async def waiting(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalized.set()
    monkeypatch.setattr(h.rt, 'vk_callback', waiting)
    task = asyncio.create_task(click(h, event(row)))
    await asyncio.wait_for(started.wait(), 5)
    h.rt.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(finalized.wait(), 5)
    assert not h.adapter.test_callbacks
    assert not h.rt.store.get(row['id'])['answer']
