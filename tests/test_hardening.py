import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter


@pytest.mark.parametrize('change', [{'transport_profile': 'other'}, {'authorization_home': Path('/another')},
                                     {'runtime_profile': 'other'}, {'runtime_home': Path('/another')}])
def test_pinned_canonical_identity_must_match_transport_and_runtime(installed, change):
    from gateway.session_identity import RoutingIdentity
    from gateway.platforms.event import MessageEvent
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('signal')
    row = rt.store.create(request('signal'), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    source = adapter.build_source(chat_id='123', user_id='456')
    fields = dict(transport_profile=rt.store.profile, runtime_profile=rt.store.profile,
                  authorization_home=Path(rt.store.home), runtime_home=Path(rt.store.home))
    source._identity = RoutingIdentity(**{**fields, **change})
    event = MessageEvent(text=f"dq {row['id']} 1", source=source)
    assert rt.pre_dispatch(event, NS(_is_user_authorized_for_source=lambda source: True)) is None
    assert rt.store.get(row['id'])['answer'] is None


def test_delivered_expiry_and_expired_idempotency_replay(installed):
    rt = runtime(installed[0])
    data = request(expires_at=time.time() + .05)
    row = rt.store.create(data, rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    time.sleep(.07)
    assert rt.store.get(row['id'])['state'] == 'expired'
    assert rt.store.answer(row['id'], '1', row['request'], authorized=True) == 'expired'
    assert rt.store.create(data, rt.platforms)['id'] == row['id']
    assert rt.store.create(data, rt.platforms)['state'] == 'expired'


def test_opt_in_rechecked_and_registry_removed_on_unload(installed, monkeypatch):
    manager, module, home = installed
    from tools.registry import registry
    rt = runtime(manager)
    entry = registry.get_entry('deferred_questions', scope=str(home))
    monkeypatch.setattr(rt.ctx, 'get_config', lambda key, default=None: False)
    assert entry.check_fn() is False
    assert json.loads(entry.handler({'action': 'request', 'request': request()})) == {'ok': False, 'error': 'tool not enabled'}
    assert rt.store.list() == []
    manager.unload('deferred-questions')
    assert registry.get_entry('deferred_questions', scope=str(home)) is None
    # Host register_platform_handler is pre-ledger and leaves inert factories on targeted unload.
    assert not rt.live
    for factory, _ in manager.get_platform_handler_factories('telegram'):
        factory(None, ControlledAdapter.make('telegram'))
    assert not rt.adapters
    assert not manager.get_slack_action_handlers()


@pytest.mark.parametrize('change', [{'chat_id': '00123'}, {'user_id': '0456'}, {'thread_id': '007'},
                                     {'chat_id': ' 123'}, {'chat_id': '１２３'}, {'user_id': '-456'}])
def test_literal_identifiers_rejected_not_repaired(installed, change):
    rt = runtime(installed[0])
    with pytest.raises(ValueError):
        rt.store.create(request(**change), rt.platforms)
    assert rt.store.list() == []
