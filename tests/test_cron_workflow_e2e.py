"""Real-host isolated integration: scheduler/CLI/native PTB/admission/agent/cron.

The installed GatewayRunner conversation/session orchestration is NOT booted:
its adapter handler is a small explicit bridge to the real AIAgent. Only the
model client and Telegram SDK network methods are controlled, not cron dispatch.
"""
import asyncio
import json
import os
import socket
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from test_deferred_questions import CORE, request, runtime
from test_review_regressions import telegram, message

sys.dont_write_bytecode = True


def child_env(home, tmp_path):
    """No inherited tokens, profile selectors, provider configuration or HOME."""
    return dict(PATH=os.environ.get('PATH', ''), HOME=str(home.parent),
                HERMES_HOME=str(home), PYTHONPATH=str(CORE), LANG='C.UTF-8',
                TMPDIR=str(tmp_path), PYTHONDONTWRITEBYTECODE='1')


def stage_producer(home, tmp_path, name, payload):
    """Test stage fixture; ordinary cron script invokes the real discovered CLI."""
    data = tmp_path / f'{name}.json'
    data.write_text(json.dumps(payload))
    (home / 'scripts').mkdir(exist_ok=True)
    producer = home / 'scripts' / f'{name}.py'
    producer.write_text(
        'import subprocess, sys\n'
        f'p = subprocess.run([sys.executable, "-m", "hermes_cli.main", '
        f'"deferred-questions", "request", "--json-file", {str(data)!r}], '
        'capture_output=True, text=True, timeout=45)\n'
        'if p.returncode: sys.stderr.write(p.stderr + p.stdout)\n'
        'raise SystemExit(p.returncode)\n')
    return producer


def run_next_job(home, tmp_path, job_id):
    """Manual actual scheduler execution, twice; not due-slot/tick bookkeeping."""
    harness = tmp_path / 'next_scheduler_probe.py'
    harness.write_text(
        'import json, sys\nfrom cron.jobs import get_job\n'
        'from cron.scheduler import run_job\n'
        f'j = get_job({job_id!r})\n'
        'assert j["no_agent"] is True\n'
        'a = run_job(j); b = run_job(j)\n'
        'assert "run_agent" not in sys.modules\n'
        'print(json.dumps([a, b]))\n')
    child = subprocess.run([sys.executable, str(harness)], cwd=tmp_path,
                           env=child_env(home, tmp_path), text=True,
                           capture_output=True, timeout=130)
    assert child.returncode == 0, child.stdout + child.stderr
    results = json.loads(child.stdout.strip().splitlines()[-1])
    assert all(r[0] and r[3] is None and r[2] == '[SILENT]' for r in results)
    assert all('silent (empty output)' in r[1] for r in results)


def produce(installed, tmp_path):
    """Persist a job; execute host run_job + script + discovered CLI in children."""
    _, _, home = installed
    payload = request(continue_agent=True, thread_id='7', options=['Walk', 'Bus', 'Finish'], question=(
        'Choose next stage. Workflow: Walk => next question "Walking distance?" '
        'in 30m; Bus => next question "Bus line?" in 2h; Finish => complete '
        'without another cron. Use cronjob_manage create, repeat=1, deliver=local.'))
    producer = stage_producer(home, tmp_path, 'producer', payload)
    harness = tmp_path / 'scheduler_probe.py'
    harness.write_text(
        'import json, sys\nfrom cron.jobs import create_job, list_jobs\n'
        'from cron.scheduler import run_job\n'
        f'j = create_job(prompt=None, schedule="1h", script={str(producer)!r}, '
        f'no_agent=True, repeat=1, deliver="local", workdir={str(tmp_path)!r})\n'
        'a = run_job(j); b = run_job(j)\n'
        'assert a[0] and b[0], (a, b)\n'
        'assert "run_agent" not in sys.modules\n'
        'print(json.dumps({"results": [a, b], "job_id": j["id"], '
        '"agent_imported": "run_agent" in sys.modules}))\n')
    # Deliberately do not inherit tokens, profile selectors, provider config or HOME.
    env = child_env(home, tmp_path)
    child = subprocess.run([sys.executable, str(harness)], cwd=tmp_path, env=env,
                           text=True, capture_output=True, timeout=130)
    assert child.returncode == 0, child.stdout + child.stderr
    result = json.loads(child.stdout.strip().splitlines()[-1])
    assert result['agent_imported'] is False
    assert all(r[0] and r[3] is None and r[2] == '[SILENT]' for r in result['results'])
    rows = runtime(installed[0]).store.list()
    assert len(rows) == 1  # same immutable occurrence, actual CLI invoked twice
    assert rows[0]['state'] == 'queued'
    return rows[0], result['job_id']


def completion(content, calls=None):
    return NS(choices=[NS(message=NS(content=content, tool_calls=calls,
                                   reasoning=None, reasoning_content=None,
                                   reasoning_details=None),
                          finish_reason='tool_calls' if calls else 'stop')],
              model='isolated-model', usage=None)


def run_real_agent(event, stages):
    """Controlled completion chooses from the delivered answer, real tool effects."""
    from run_agent import AIAgent
    calls = []
    def respond(**kwargs):
        messages = kwargs['messages']
        text = next(m['content'] for m in messages if m['role'] == 'user')
        assert event.text == text
        receipts = [m for m in messages if m['role'] == 'tool']
        if receipts and receipts[-1]['tool_call_id'] != 'describe-cron':
            assert json.loads(receipts[-1]['content']).get('success') is True, receipts
            return completion('Next stage scheduled.')
        if '\nAnswer: Finish\n' in text:
            return completion('Workflow complete; no next cron.')
        if not receipts:
            return completion(None, [NS(id='describe-cron', type='function', function=NS(
                name='tool_describe', arguments=json.dumps({'names': ['cronjob_manage']})))])
        assert 'cronjob_manage' in receipts[-1]['content']
        option, schedule, question = ('Walk', '30m', 'Walking distance?') if '\nAnswer: Walk\n' in text else ('Bus', '2h', 'Bus line?')
        assert f'\nAnswer: {option}\n' in text
        arguments = dict(action='create', name='adaptive-next', schedule=schedule,
                         script=stages[option].name, no_agent=True,
                         repeat=1, deliver='local')
        calls.append(arguments)
        return completion(None, [NS(id='controlled-cron-call', type='function',
                                   function=NS(name='tool_call', arguments=json.dumps({'calls': [
                                       {'name': 'cronjob_manage', 'arguments': arguments}]})))])
    # No mock of tool discovery, model_tools dispatch, cron handler or job store.
    with patch('agent.process_bootstrap.OpenAI'):
        fixture_key = '-'.join(('isolated', 'test', 'not', 'a', 'secret'))
        agent = AIAgent(api_key=fixture_key,
                        base_url='https://openrouter.ai/api/v1', model='isolated-model',
                        enabled_toolsets=['cronjob'], quiet_mode=True,
                        skip_context_files=True, skip_memory=True, max_iterations=4)
    agent.client = MagicMock()
    agent.client.chat.completions.create.side_effect = respond
    agent._cached_system_prompt = 'Execute the supplied workflow using available tools.'
    agent._use_prompt_caching = False
    agent.compression_enabled = False
    agent.save_trajectories = False
    try:
        result = agent.run_conversation(event.text)
        assert result['completed'] is True, result
        return result, calls
    finally:
        agent.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('option,schedule,question', [
    (1, 30 * 60, 'Walking distance?'), (2, 2 * 60 * 60, 'Bus line?'),
    (3, None, None),
])
async def test_script_question_answer_real_agent_adaptive_effect(installed, tmp_path, monkeypatch, option, schedule, question):
    from telegram import Bot, Update
    from telegram.ext import Application
    from cron.jobs import list_jobs
    from datetime import datetime
    rt = runtime(installed[0])
    row, initial_job = produce(installed, tmp_path)
    home = installed[2]
    next_options = {'Walk': ['Short walk', 'Long walk'],
                    'Bus': ['Line 5', 'Line 12', 'Express']}
    stages = {choice: stage_producer(home, tmp_path, f'next-{choice.lower()}',
              request(continue_agent=True, thread_id='7', question=next_question,
                      options=next_options[choice], idempotency_key=f'next-{row["id"]}-{choice}'))
              for choice, next_question in [('Walk', 'Walking distance?'), ('Bus', 'Bus line?')]}
    monkeypatch.setenv('HERMES_GATEWAY_SESSION', '1')
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: (_ for _ in ()).throw(AssertionError('Unexpected network')))
    adapter = telegram()
    bot = Bot('123456:ISOLATED-TEST-TOKEN')
    app = Application.builder().bot(bot).build()
    # Actual native handler factory binding; stop its polling task for explicit barriers.
    rt.wire('telegram', app, adapter)
    rt.workers['telegram'].cancel()
    await asyncio.gather(rt.workers['telegram'], return_exceptions=True)
    done = asyncio.Event()
    seen, results, errors = [], [], []
    async def inbound(event):
        seen.append(event)
        try:
            results.append(await asyncio.to_thread(run_real_agent, event, stages))
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()
        return None
    adapter.set_message_handler(inbound)
    with patch.object(Bot, 'send_message', new=AsyncMock(side_effect=[message(bot), message(bot, mid=43)])) as send, \
         patch.object(Bot, 'edit_message_text', new=AsyncMock(return_value=True)) as edit, \
         patch.object(Bot, 'answer_callback_query', new=AsyncMock()):
        await rt.dispatch_once('telegram')
        assert send.await_count == 1
        assert rt.store.get(row['id'])['message_id'] == '42'
        keyboard = send.call_args.kwargs['reply_markup']
        token = keyboard.inline_keyboard[option - 1][0].callback_data
        incoming = message(bot).to_dict()
        incoming['from'] = {'id': 999, 'first_name': 'Bot', 'is_bot': True}
        update = Update.de_json({'update_id': 1, 'callback_query': {
            'id': 'isolated-click', 'chat_instance': 'test',
            'from': {'id': 456, 'first_name': 'User', 'is_bot': False},
            'message': incoming, 'data': token}}, bot)
        handler = app.handlers[-20][0]
        await handler.handle_update(update, app, handler.check_update(update), NS())
        assert rt.store.get(row['id'])['answer']['value'] == row['request']['options'][option - 1]
        await rt.answer_feedback(row['id'])
        assert rt.store.feedback(row['id'])['state'] == 'done'
        assert edit.await_count == 1 and edit.call_args.kwargs['reply_markup'] is None
        await rt.dispatch_answers_once('telegram')  # real gateway.wake/base admission
        await asyncio.wait_for(done.wait(), 60)
        assert not errors, errors
        # Duplicate native click + dispatcher passes must not produce a second turn.
        await handler.handle_update(update, app, handler.check_update(update), NS())
        rt.store = type(rt.store)(rt.store.home, rt.store.profile)
        await rt.dispatch_answers_once('telegram')
        await rt.dispatch_once('telegram')
        await asyncio.sleep(0)
        assert send.await_count == 1 and len(seen) == 1
        assert rt.store.feedback(row['id'])['state'] == 'done'
        assert rt.store.get(row['id'])['answer']['value'] == row['request']['options'][option - 1]
        jobs = [j for j in list_jobs(include_disabled=True) if j['id'] != initial_job]
        if schedule is None:
            assert len(rt.store.list()) == 1
            assert jobs == []
        else:
            assert len(jobs) == 1
            selected = row['request']['options'][option - 1]
            assert jobs[0]['script'] == stages[selected].name
            assert jobs[0]['no_agent'] is True and not jobs[0]['prompt']
            await asyncio.to_thread(run_next_job, home, tmp_path, jobs[0]['id'])
            rows = rt.store.list()
            assert len(rows) == 2  # two actual CLI runs create one next occurrence
            second = next(r for r in rows if r['id'] != row['id'])
            assert second['state'] == 'queued'
            assert send.await_count == 1  # producer queues only; worker owns sending
            assert second['request']['question'] == question
            assert second['request']['options'] == next_options[selected]
            other = 'Bus' if selected == 'Walk' else 'Walk'
            assert second['request']['options'] != next_options[other]
            assert second['request']['thread_id'] == '7'
            await rt.dispatch_once('telegram')
            second = rt.store.get(second['id'])
            assert second['state'] == 'sent' and second['message_id'] == '43'
            assert rt.store.get(row['id'])['message_id'] == '42'
            assert send.await_count == 2
            outgoing = send.call_args.kwargs
            assert outgoing['chat_id'] == '123' and outgoing['message_thread_id'] == 7
            assert 'direct_messages_topic_id' not in outgoing
            assert question in outgoing['text']
            labels = [b.text for buttons in outgoing['reply_markup'].inline_keyboard for b in buttons]
            assert labels == next_options[selected]
            rt.store = type(rt.store)(rt.store.home, rt.store.profile)
            await rt.dispatch_once('telegram')
            assert len(rt.store.list()) == 2 and send.await_count == 2
            assert len(seen) == 1  # the next question has not been answered
    assert seen[0].internal and not seen[0].allow_gateway_control
    assert seen[0].metadata['deferred_question_id'] == row['id']
    jobs = [j for j in list_jobs(include_disabled=True) if j['id'] != initial_job]
    if schedule is None:
        assert jobs == [] and results[0][1] == []
        assert results[0][0]['final_response'] == 'Workflow complete; no next cron.'
    else:
        assert len(jobs) == 1 and jobs[0]['no_agent'] is True
        assert len(results[0][1]) == 1
        created = datetime.fromisoformat(jobs[0]['created_at'])
        next_run = datetime.fromisoformat(jobs[0]['next_run_at'])
        assert abs((next_run - created).total_seconds() - schedule) < 5
        assert jobs[0]['repeat']['times'] == 1 and jobs[0]['deliver'] == 'local'
    assert rt.store.pending_actions('telegram') == []


@pytest.mark.asyncio
async def test_admitted_before_effect_can_be_lost_no_unsafe_replay(installed):
    """Cancel process-local handler after admission: persisted answer != cron effect."""
    from cron.jobs import list_jobs
    rt = runtime(installed[0])
    adapter = telegram()
    entered = asyncio.Event()
    parked = asyncio.Event()
    tasks = []
    async def blocked(event):
        tasks.append(asyncio.current_task())
        entered.set()
        await parked.wait()
        raise AssertionError('Crash boundary must not reach effects')
    adapter.set_message_handler(blocked)
    rt.adapters['telegram'] = (None, adapter)
    row = rt.store.create(request(continue_agent=True), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    assert rt.store.answer(row['id'], '2', row['request'], authorized=True) == 'answered'
    await rt.dispatch_answers_once('telegram')
    await asyncio.wait_for(entered.wait(), 3)
    assert rt.store.pending_actions('telegram') == []
    with rt.store.connect() as db:
        assert db.execute('SELECT state FROM answer_actions WHERE question_id=?',
                          (row['id'],)).fetchone()[0] == 'admitted'
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    rt.store = type(rt.store)(rt.store.home, rt.store.profile)
    await rt.dispatch_answers_once('telegram')
    assert len(tasks) == 1 and list_jobs(include_disabled=True) == []
    assert rt.store.get(row['id'])['answer']['value'] == 'Bus'
