"""Isolated runtime/SDK boundary tests. Run via Hermes scripts/run_tests.sh ONLY."""
import argparse
import asyncio
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

CORE = Path(os.environ['HERMES_CORE_ROOT']).resolve()
sys.path.insert(0, str(CORE))
PROJECT = Path(__file__).resolve().parents[1]


@pytest.fixture
def installed(tmp_path, monkeypatch):
    home = tmp_path / 'home' / '.hermes'
    home.mkdir(parents=True)
    monkeypatch.setenv('HOME', str(home.parent))
    monkeypatch.setenv('HERMES_HOME', str(home))
    target = home / 'plugins' / 'deferred-questions'
    target.mkdir(parents=True)
    shutil.copy2(PROJECT / 'plugin.yaml', target / 'plugin.yaml')
    # Preserve the legacy directory-loader regression surface (evicted modules).
    # scripts/verify_install.py separately exercises the actual public bridge,
    # stable package and real catalog clone/wheel in a disposable interpreter.
    for name in ('__init__.py', 'store.py', 'runtime.py', 'lifecycle.py'):
        shutil.copy2(PROJECT / 'hermes_deferred_questions' / name, target / name)
    home.joinpath('config.yaml').write_text('plugins:\n  enabled: [deferred-questions]\n  entries:\n    deferred-questions:\n      settings:\n        enable_tool: true\n')
    from hermes_cli.plugins import PluginManager
    manager = PluginManager(scope_key=str(home))
    manager.discover_and_load()
    loaded = manager._plugins['deferred-questions']
    assert loaded.enabled, loaded.error
    module = importlib.import_module('hermes_plugins.deferred_questions')
    yield manager, module, home
    manager.unload()


def request(platform='telegram', **changes):
    result = dict(platform=platform, chat_id='123', user_id='456', chat_type='dm',
                  question='Which route today?', options=['Walk', 'Bus', 'Stay home'],
                  expires_at=time.time() + 3600, idempotency_key='test-1')
    result.update(changes)
    return result


def cli(manager, *argv):
    entry = manager._cli_commands['deferred-questions']
    parser = argparse.ArgumentParser()
    entry['setup_fn'](parser)
    entry['handler_fn'](parser.parse_args(argv))


def runtime(manager):
    return manager._hooks['pre_gateway_dispatch'][0].__self__


def store(installed):
    manager, module, home = installed
    rt = runtime(manager)
    return rt.store


class ControlledAdapter:
    """Real base adapter methods, with network sends controlled at the SDK boundary."""
    @staticmethod
    def make(platform, home=None):
        from gateway.platforms.base import BasePlatformAdapter
        from gateway.config import Platform, PlatformConfig
        # Generic native boundary subclass, no standalone bot/client/polling session.
        class Adapter(BasePlatformAdapter):
            async def connect(self): return True
            async def disconnect(self): pass
            async def get_chat_info(self, chat_id): return {}
            async def send(self, chat_id, content, reply_to=None, metadata=None):
                from gateway.platforms.base import SendResult
                self.sent.append((chat_id, content, metadata))
                return SendResult(success=True, message_id='msg-1')
        adapter = Adapter(PlatformConfig(enabled=True), Platform(platform))
        adapter._running = True
        adapter.sent = []
        if platform == 'telegram':
            # Controlled local-intake seam; review regressions use real TelegramAdapter.
            adapter._is_user_authorized_from_message = lambda message: True
        adapter.set_authorization_check(lambda user, chat_type, chat_id, **kw: user == '456')
        return adapter


def test_loader_cli_tool_and_durable_snapshot(installed, tmp_path, capsys):
    manager, module, home = installed
    data = request(options=['A', 'B', 'C', 'D', 'E', 'F', 'G'])
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(data))
    cli(manager, 'request', '--json-file', str(path))
    result = json.loads(capsys.readouterr().out)
    assert result['ok']
    row = result['result']
    assert row['state'] == 'queued'
    assert len(row['snapshot']['buttons']) == 7
    assert row['snapshot']['buttons'][6]['token'] == f"dq:{row['id']}:7"
    data['options'][0] = 'MUTATED'
    assert store(installed).get(row['id'])['request']['options'][0] == 'A'
    from tools.registry import registry
    entry = registry.get_entry('deferred_questions', scope=str(home))
    assert entry is not None
    response = json.loads(entry.handler({'action': 'get', 'id': row['id']}))
    assert response['result']['snapshot'] == row['snapshot']
    assert entry.check_fn() is True
    assert store(installed).path.stat().st_mode & 0o777 == 0o600


def test_subprocess_cli_shared_queue(installed, tmp_path):
    manager, module, home = installed
    data = request()
    path = tmp_path / 'request.json'
    path.write_text(json.dumps(data))
    env = {'PATH': os.environ.get('PATH', ''), 'HOME': str(home.parent), 'HERMES_HOME': str(home),
           'PYTHONPATH': str(CORE), 'LANG': 'C.UTF-8', 'TMPDIR': str(tmp_path)}
    # Actual Hermes CLI parser + plugin command discovery in a separate interpreter.
    command = [sys.executable, '-m', 'hermes_cli.main', 'deferred-questions', 'request', '--json-file', str(path)]
    done = subprocess.run(command, cwd=CORE, env=env, capture_output=True, text=True, timeout=45)
    assert done.returncode == 0, done.stderr + done.stdout
    parsed = json.loads(done.stdout.strip().splitlines()[-1])
    assert parsed['ok']
    assert store(installed).get(parsed['result']['id'])['state'] == 'queued'
    again = subprocess.run(command, cwd=CORE, env=env, capture_output=True, text=True, timeout=45)
    assert again.returncode == 0
    assert json.loads(again.stdout.strip().splitlines()[-1])['result']['id'] == parsed['result']['id']


@pytest.mark.asyncio
async def test_generic_dispatch_real_adapter_event_hook(installed):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('signal')
    row = rt.store.create(request('signal'), rt.platforms)
    # Real PluginManager factory -> Runtime worker registration.
    factory = manager.get_platform_handler_factories('signal')[0][0]
    factory(None, adapter)
    await rt.dispatch_once('signal')
    assert rt.store.get(row['id'])['state'] == 'sent'
    assert len(adapter.sent) == 1
    await rt.dispatch_once('signal')
    assert len(adapter.sent) == 1
    from gateway.platforms.event import MessageEvent
    source = adapter.build_source(chat_id='123', user_id='456', chat_type='dm')
    event = MessageEvent(text=f"dq {row['id']} 3", source=source)
    gateway = NS(_is_user_authorized_for_source=lambda source: True)
    result = rt.pre_dispatch(event, gateway)
    assert result == {'action': 'skip', 'reason': 'deferred-question:answered'}
    assert rt.store.get(row['id'])['answer']['value'] == 'Stay home'
    reopened = module.store.Store(home, rt.store.profile)
    assert reopened.get(row['id'])['answer']['index'] == 3
    manager.unload('deferred-questions')
    await asyncio.sleep(0)
    assert not rt.live
    assert all(t.done() for t in rt.workers.values())


@pytest.mark.parametrize('change', [{'user_id': 'other'}, {'chat_id': 'other'}, {'thread_id': 'other'},
                                     {'scope_id': 'other'}, {'profile': 'other'}, {'profile_home': '/wrong'},
                                     {'chat_type': 'group'}, {'parent_chat_id': 'other'}])
def test_exact_route_rejection(installed, change):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id'])
    rt.store.receipt(row['id'], '42')
    route = {**row['request'], **change}
    assert rt.store.answer(row['id'], '1', route, authorized=True) == 'wrong_route'
    assert rt.store.get(row['id'])['answer'] is None


def test_pre_auth_does_not_consume_or_accept(installed):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource
    from gateway.config import Platform
    event = MessageEvent(text=f"dq {row['id']} 1", source=SessionSource(Platform.TELEGRAM, '123', user_id='456'))
    for checker in (lambda source: False, lambda source: 1, lambda source: None):
        assert rt.pre_dispatch(event, NS(_is_user_authorized_for_source=checker)) is None
    assert rt.pre_dispatch(event, NS()) is None
    event.internal = True
    assert rt.pre_dispatch(event, NS(_is_user_authorized_for_source=lambda source: True)) is None
    assert rt.store.get(row['id'])['answer'] is None


def test_first_write_unknown_expiry_and_cancel(installed):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    assert rt.store.answer(row['id'], '1', row['request'], authorized=True) == 'not_delivered'
    assert rt.store.claim(row['id'])
    assert not rt.store.claim(row['id'])
    rt.store.receipt(row['id'], '42')
    assert rt.store.answer(row['id'], '1', row['request'], authorized=True, message_id='43') == 'wrong_message'
    assert rt.store.answer(row['id'], '1', row['request'], authorized=True, message_id='42') == 'answered'
    assert rt.store.answer(row['id'], '2', row['request'], authorized=True) == 'already_answered'
    unknown = rt.store.create(request(idempotency_key='uncertain'), rt.platforms)
    rt.store.claim(unknown['id'])
    reopened = installed[1].store.Store(installed[2], rt.store.profile)
    assert not reopened.claim(unknown['id'])
    assert reopened.get(unknown['id'])['state'] == 'sending'
    rt.store.receipt(unknown['id'], diagnostic='transport_exception')
    assert not rt.store.claim(unknown['id'])
    assert not rt.store.cancel(unknown['id'])
    expiring = rt.store.create(request(idempotency_key='expiry', expires_at=time.time() + .03), rt.platforms)
    time.sleep(.05)
    assert not rt.store.claim(expiring['id'])
    assert rt.store.get(expiring['id'])['state'] == 'expired'
    cancelled = rt.store.create(request(idempotency_key='cancel'), rt.platforms)
    assert rt.store.cancel(cancelled['id'])
    assert not rt.store.claim(cancelled['id'])


@pytest.mark.parametrize('change', [{'options': ['duplicate', 'duplicate']}, {'user_id': ''}, {'options': [1]},
                                     {'expires_at': float('nan')}, {'expires_at': 1}, {'allow_text': 'true'},
                                     {'question': '🚀' * 1200}, {'options': [], 'allow_text': False},
                                     {'thread_id': 1}, {'parent_chat_id': []}, {'unknown': True}])
def test_invalid_request(installed, change):
    rt = runtime(installed[0])
    with pytest.raises(ValueError):
        rt.store.create(request(**change), rt.platforms)


def test_idempotency_conflict_custom_text_and_ambiguity(installed):
    rt = runtime(installed[0])
    data = request()
    row = rt.store.create(data, rt.platforms)
    assert rt.store.create(data, rt.platforms)['id'] == row['id']
    with pytest.raises(ValueError, match='different request'):
        rt.store.create({**data, 'question': 'Different'}, rt.platforms)
    other = rt.store.create(request(idempotency_key='second'), rt.platforms)
    for item in (row, other):
        rt.store.claim(item['id']); rt.store.receipt(item['id'], item['id'])
    from gateway.platforms.event import MessageEvent
    adapter = ControlledAdapter.make('telegram')
    event = MessageEvent(text='1', source=adapter.build_source(chat_id='123', user_id='456'))
    assert rt.pre_dispatch(event, NS(_is_user_authorized_for_source=lambda source: True)) is None
    assert rt.store.answer(row['id'], '99', row['request'], authorized=True) == 'invalid_answer'
    assert rt.store.answer(row['id'], 'text:  Custom  ', row['request'], authorized=True) == 'answered'
    assert rt.store.get(other['id'])['answer'] is None


@pytest.mark.asyncio
async def test_telegram_actual_ptb_handler_receipt_and_restart(installed, monkeypatch):
    from telegram import Bot, Update
    from telegram.ext import Application
    manager, module, home = installed
    rt = runtime(manager)
    bot = Bot('123456:ISOLATED-TEST-TOKEN')
    app = Application.builder().bot(bot).build()
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from gateway.config import PlatformConfig
    adapter = TelegramAdapter(PlatformConfig(enabled=True))
    adapter._running = True
    adapter.set_authorization_check(lambda user, chat_type, chat_id, **kw: user == '456')
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    sent = []
    async def send(**kwargs):
        sent.append(kwargs)
        return NS(message_id=42, chat=NS(id=123), message_thread_id=None)
    # SDK method is mocked; real SDK keyboard, update decoder and handler selection remain.
    from unittest.mock import patch
    row = rt.store.create(request(), rt.platforms)
    with patch.object(Bot, 'send_message', new=AsyncMock(side_effect=send)):
        adapter._wire_plugin_handlers(app)
        await rt.dispatch_once('telegram')
    assert rt.store.get(row['id'])['message_id'] == '42'
    assert sent[0]['reply_markup'].inline_keyboard[2][0].callback_data == f"dq:{row['id']}:3"
    # Re-register a new runtime after unload, then feed old message's persisted token.
    manager.unload('deferred-questions')
    await asyncio.sleep(0)
    from hermes_cli.plugins import PluginManager
    second = PluginManager(scope_key=str(home)); second.discover_and_load()
    rt2 = runtime(second)
    app2 = Application.builder().bot(bot).build()
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: second)
    adapter._wire_plugin_handlers(app2)
    update = Update.de_json({'update_id': 7, 'callback_query': {'id': 'cb7', 'chat_instance': 'chat7',
                             'from': {'id': 456, 'first_name': 'User', 'is_bot': False},
                             'message': {'message_id': 42, 'date': 1, 'chat': {'id': 123, 'type': 'private'}},
                             'data': f"dq:{row['id']}:2"}}, bot)
    handler = app2.handlers[-20][0]
    assert handler.check_update(update)
    with patch.object(Bot, 'answer_callback_query', new=AsyncMock()) as ack:
        await handler.handle_update(update, app2, handler.check_update(update), NS())
        assert ack.await_count == 1
    assert rt2.store.get(row['id'])['answer']['value'] == 'Bus'
    second.unload(); await asyncio.sleep(0)
    assert -20 not in app2.handlers or not app2.handlers[-20]


@pytest.mark.asyncio
async def test_slack_actual_action_registration_and_payload(installed):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('slack')
    client = NS(chat_postMessage=AsyncMock(return_value={'ok': True, 'channel': 'D123', 'ts': '42.0'}))
    adapter._team_clients = {'T1': client}
    row = rt.store.create(request('slack', chat_id='D123', scope_id='T1'), rt.platforms)
    rt.wire('slack', NS(), adapter)
    await rt.dispatch_once('slack')
    args = client.chat_postMessage.call_args.kwargs
    assert args['channel'] == 'D123'
    assert args['blocks'][1]['elements'][0]['action_id'] == 'hermes_dq_0'
    matcher, callback, _ = manager.get_slack_action_handlers()[0]
    assert matcher.match('hermes_dq_0')
    ack = AsyncMock()
    body = {'team': {'id': 'T1'}, 'channel': {'id': 'D123'}, 'user': {'id': '456'},
            'message': {'ts': '42.0'}}
    await callback(ack, body, {'value': f"dq:{row['id']}:1"})
    assert ack.await_count == 1
    assert rt.store.get(row['id'])['answer']['value'] == 'Walk'


@pytest.mark.asyncio
async def test_unknown_transport_failure_no_retry(installed):
    rt = runtime(installed[0])
    adapter = ControlledAdapter.make('signal')
    adapter.send = AsyncMock(side_effect=TimeoutError())
    row = rt.store.create(request('signal'), rt.platforms)
    rt.wire('signal', None, adapter)
    await rt.dispatch_once('signal'); await rt.dispatch_once('signal')
    assert adapter.send.await_count == 1
    assert rt.store.get(row['id'])['state'] == 'unknown'
