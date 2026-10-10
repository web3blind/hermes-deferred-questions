"""Cross-repository integration with the public VK generic callback implementation."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

from test_vk_callbacks import event, harness
from test_vk_intake import new_question, sent_args


@pytest.fixture
def public_vk(monkeypatch):
    root = os.environ.get('HERMES_VK_PLUGIN_ROOT')
    path = (Path(root) if root else Path(os.environ['HERMES_HOME']) / 'plugins' / 'vk') / 'adapter.py'
    if not path.exists():
        pytest.skip('public VK checkout is required for cross-repository integration')
    spec = importlib.util.spec_from_file_location('dq_public_vk_integration', path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr('plugins.platforms.vk.adapter.VKAdapter', module.VKAdapter)
    return module.VKAdapter


async def native_click(h, obj):
    await h.adapter._handle_message_event_update({'type': 'message_event', 'object': obj})
    await asyncio.gather(*[task for task in h.rt._tasks['vk'] if not task.done()])


async def setup(installed, monkeypatch, **kwargs):
    h = await harness(installed, monkeypatch, **kwargs)
    original = h.adapter._vk_method
    async def vk_method(name, params):
        if name == 'messages.sendMessageEventAnswer':
            h.calls.append((name, params))
            return {'response': 1}
        return await original(name, params)
    h.adapter._vk_method = vk_method
    return h


@pytest.mark.asyncio
@pytest.mark.parametrize('peer,thread', [('123', None), ('2000000123', None),
                                       ('123', 'lane:a'), ('2000000123', 'lane:a')])
async def test_public_adapter_message_event_callback(installed, monkeypatch, public_vk, peer, thread):
    h = await setup(installed, monkeypatch, peer=peer)
    assert type(h.adapter) is public_vk
    row = new_question(h, thread)
    await h.adapter._set_active_lane_id(peer, '456', 'b')
    await h.adapter._set_pending_edit(peer, '456', 'b')
    await h.rt.dispatch_once('vk')
    action = json.loads(sent_args(h)['keyboard'])['buttons'][0][1]['action']
    assert action['type'] == 'callback' and action['label'] == 'Bus'
    obj = event(row, peer=peer)
    obj['payload'] = action['payload']  # real decoder passes both payload and data
    await native_click(h, obj)
    assert h.rt.store.get(row['id'])['answer']['value'] == 'Bus'
    assert h.rt.store.feedback(row['id'])['state'] == 'done'
    assert h.adapter._get_active_lane_id(peer, '456') == 'b'
    assert h.adapter._pending_edit(peer, '456', prune=False)
    acknowledgments = [params for name, params in h.calls if name == 'messages.sendMessageEventAnswer']
    assert len(acknowledgments) == 1
    assert acknowledgments[0]['event_id'] == obj['event_id']
    assert json.loads(acknowledgments[0]['event_data'])['text'] == 'Ответ сохранён'
    assert not h.seen and not h.forwarded


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['acl', 'actor', 'peer', 'receipt', 'lane', 'profile', 'injected-lane',
                                 'parent', 'rejected', 'authorization-home', 'runtime-home',
                                 'transport-profile', 'runtime-profile'])
async def test_public_callback_fail_closed(installed, monkeypatch, public_vk, case):
    h = await setup(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    obj = event(row)
    if case == 'acl':
        obj['user_id'] = 888
    if case == 'actor':
        obj['user_id'] = 789
    if case == 'peer':
        obj['peer_id'] = 2000000999
    if case == 'receipt':
        obj['conversation_message_id'] = 999
    if case == 'lane':
        h.adapter._lane_state['message_lanes'].clear()
    if case == 'profile':
        h.adapter._owner_profile = 'other'
    if case == 'injected-lane':
        obj['payload']['data']['thread_id'] = 'lane:a'
    if case in ('parent', 'rejected', 'authorization-home', 'runtime-home',
                'transport-profile', 'runtime-profile'):
        from gateway.session_identity import RoutingIdentity
        build = h.adapter.build_source
        def source_with_actual_identity(**kwargs):
            source = build(**kwargs)
            home, profile = Path(h.rt.store.home), h.rt.store.profile
            source._identity = RoutingIdentity(
                transport_profile='other' if case == 'transport-profile' else profile,
                runtime_profile='other' if case == 'runtime-profile' else profile,
                authorization_home=home / 'other' if case == 'authorization-home' else home,
                runtime_home=home / 'other' if case == 'runtime-home' else home)
            if case == 'parent':
                source.parent_chat_id = 'unexpected'
            if case == 'rejected':
                source.profile_route_rejected = True
            return source
        monkeypatch.setattr(h.adapter, 'build_source', source_with_actual_identity)
    await native_click(h, obj)
    assert not h.rt.store.get(row['id'])['answer']
    assert not h.rt.store.feedback(row['id'])
    assert not h.forwarded and not h.seen


@pytest.mark.asyncio
async def test_public_listener_cleanup_reload_persisted_receipt(installed, monkeypatch, public_vk):
    h = await setup(installed, monkeypatch)
    row = new_question(h)
    await h.rt.dispatch_once('vk')
    obj = event(row)
    assert 'deferred_questions' in h.adapter._callback_handlers
    h.rt.close()
    await asyncio.sleep(.05)
    assert 'deferred_questions' not in h.adapter._callback_handlers
    await native_click(h, obj)
    assert not h.rt.store.get(row['id'])['answer']
    installed[0].discover_and_load(force=True)
    new = await setup(installed, monkeypatch)
    await native_click(new, obj)
    assert new.rt.store.get(row['id'])['answer']['value'] == 'Bus'


@pytest.mark.asyncio
async def test_public_inflight_callback_unload_cancels_owned_task(installed, monkeypatch, public_vk):
    h = await setup(installed, monkeypatch)
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
    task = asyncio.create_task(h.adapter._handle_message_event_update(
        {'type': 'message_event', 'object': event(row)}))
    await asyncio.wait_for(started.wait(), 5)
    h.rt.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(finalized.wait(), 5)
    assert 'deferred_questions' not in h.adapter._callback_handlers
    assert not h.rt.store.get(row['id'])['answer']
