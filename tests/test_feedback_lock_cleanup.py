"""Terminal cleanup and real process overlap against an isolated ledger."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_deferred_questions import CORE, PROJECT, ControlledAdapter, request, runtime
from test_answer_feedback import answered


@pytest.mark.asyncio
@pytest.mark.parametrize('unchanged', [False, True])
async def test_done_and_duplicate_remove_question_lock(installed, unchanged):
    from telegram.error import BadRequest
    rt = runtime(installed[0]); qid = answered(rt)
    edit = AsyncMock(side_effect=BadRequest('Message is not modified') if unchanged else None)
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=edit)), ControlledAdapter.make('telegram'))
    path = rt.store.path.with_name(f'feedback-{qid}.lock')
    assert await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['state'] == 'done'
    assert not path.exists()
    guard = rt.store.path.with_name('feedback.lock')
    inode = guard.stat().st_ino
    assert await rt.answer_feedback(qid)
    assert not path.exists() and guard.stat().st_ino == inode
    assert edit.await_count == 1


@pytest.mark.asyncio
async def test_cancelled_question_feedback_removes_lock(installed):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    qid = row['id']
    rt.store.claim(qid); rt.store.receipt(qid, '42')
    assert rt.store.cancel(qid)
    edit = AsyncMock()
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=edit)), ControlledAdapter.make('telegram'))
    assert await rt.answer_feedback(qid)
    assert 'Question cancelled.' in edit.call_args.kwargs['text']
    assert rt.store.feedback(qid)['state'] == 'done'
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()


@pytest.mark.asyncio
async def test_failed_unlink_preserves_receipt_and_retries_cleanup(installed, monkeypatch):
    rt = runtime(installed[0]); qid = answered(rt)
    edit = AsyncMock()
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=edit)), ControlledAdapter.make('telegram'))
    path = rt.store.path.with_name(f'feedback-{qid}.lock')
    unlink = Path.unlink
    def deny(target, *args, **kwargs):
        if target == path:
            raise PermissionError('isolated cleanup failure')
        return unlink(target, *args, **kwargs)
    with monkeypatch.context() as patcher:
        patcher.setattr(Path, 'unlink', deny)
        assert await rt.answer_feedback(qid)
    assert path.exists() and rt.store.feedback(qid)['state'] == 'done'
    assert await rt.answer_feedback(qid)
    assert not path.exists() and edit.await_count == 1


@pytest.mark.asyncio
async def test_stale_completion_does_not_unlink_retry_lock(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    async def edit(**kwargs):
        with rt.store.connect() as db:
            db.execute('UPDATE feedback_edits SET version=version+1 WHERE question_id=?', (qid,))
    bot = NS(edit_message_text=AsyncMock(side_effect=edit))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    path = rt.store.path.with_name(f'feedback-{qid}.lock')
    assert not await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['state'] == 'pending' and path.exists()
    bot.edit_message_text.side_effect = None
    assert await rt.answer_feedback(qid)
    assert not path.exists() and bot.edit_message_text.await_count == 2


CHILD = '''
import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock
from hermes_cli.plugins import PluginManager
from test_deferred_questions import runtime, ControlledAdapter
manager = PluginManager()
manager.discover_and_load()
rt = runtime(manager)
qid, entered, release, mode = sys.argv[1:]
if mode == 'cleanup-hold':
    feedback = rt.store.feedback
    def held_feedback(question_id):
        intent = feedback(question_id)
        if intent is not None and intent['state'] == 'done':
            import time
            Path(entered + '.cleanup').write_text('cleanup holds admission guard')
            while not Path(release).exists():
                time.sleep(.01)
        return intent
    rt.store.feedback = held_feedback
if mode == 'probe':
    Path(entered + '.start').write_text('about to enter feedback admission')
async def edit(**kwargs):
    Path(entered).write_text('edit entered')
    if mode == 'hold':
        while not Path(release).exists():
            await asyncio.sleep(.01)
rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=AsyncMock(side_effect=edit))), ControlledAdapter.make('telegram'))
print(asyncio.run(rt.answer_feedback(qid)), flush=True)
manager.unload()
'''


def child(installed, qid, entered, release, mode):
    env = dict(os.environ, HOME=str(installed[2].parent), HERMES_HOME=str(installed[2]),
               HERMES_CORE_ROOT=str(CORE),
               PYTHONPATH=os.pathsep.join((str(PROJECT / 'tests'), str(CORE))))
    return subprocess.Popen([sys.executable, '-c', CHILD, qid, str(entered), str(release), mode],
                            cwd=installed[2], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


async def wait_entered(process, path):
    async def wait():
        while not path.exists():
            assert process.poll() is None, process.communicate()
            await asyncio.sleep(.01)
    await asyncio.wait_for(wait(), 15)


async def finish(process):
    stdout, stderr = await asyncio.to_thread(process.communicate, timeout=15)
    assert process.returncode == 0, stdout + stderr
    return stdout.strip().splitlines()[-1]


@pytest.mark.asyncio
async def test_admission_cannot_open_old_inode_during_terminal_cleanup(installed, tmp_path):
    rt = runtime(installed[0]); qid = answered(rt)
    path = rt.store.path.with_name(f'feedback-{qid}.lock')
    entered, release = tmp_path / 'entered', tmp_path / 'release'
    owner = child(installed, qid, entered, release, 'cleanup-hold')
    contender = None
    try:
        await wait_entered(owner, Path(str(entered) + '.cleanup'))
        assert rt.store.feedback(qid)['state'] == 'done'
        inode = path.stat().st_ino
        contender_entry = tmp_path / 'contender'
        contender = child(installed, qid, contender_entry, release, 'probe')
        await wait_entered(contender, Path(str(contender_entry) + '.start'))
        assert await finish(contender) == 'False'
        assert not contender_entry.exists() and path.stat().st_ino == inode
        release.write_text('cleanup may unlink')
        assert await finish(owner) == 'True'
        assert await rt.answer_feedback(qid)  # Conservative busy result is retryable.
        assert not contender_entry.exists() and not path.exists()
    finally:
        for process in (owner, contender):
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=15)


@pytest.mark.asyncio
@pytest.mark.parametrize('kill_owner', [False, True])
async def test_process_overlap_and_crash_retry_keep_one_lock_inode(installed, tmp_path, kill_owner):
    rt = runtime(installed[0]); qid = answered(rt)
    path = rt.store.path.with_name(f'feedback-{qid}.lock')
    entered, release = tmp_path / 'entered', tmp_path / 'release'
    owner = child(installed, qid, entered, release, 'hold')
    others = []
    try:
        await wait_entered(owner, entered)
        inode = path.stat().st_ino
        loser_entry = tmp_path / 'loser-entry'
        contender = child(installed, qid, loser_entry, release, 'once'); others.append(contender)
        assert await finish(contender) == 'False'
        assert not loser_entry.exists() and path.stat().st_ino == inode
        if kill_owner:
            owner.kill()
            await asyncio.to_thread(owner.communicate, timeout=15)
            assert rt.store.feedback(qid)['state'] == 'pending'
            assert path.stat().st_ino == inode
            retry_entry = tmp_path / 'retry-entry'
            retry = child(installed, qid, retry_entry, release, 'once'); others.append(retry)
            assert await finish(retry) == 'True' and retry_entry.exists()
        else:
            release.write_text('finish')
            assert await finish(owner) == 'True'
        assert rt.store.feedback(qid)['state'] == 'done' and not path.exists()
        duplicate_entry = tmp_path / 'duplicate-entry'
        duplicate = child(installed, qid, duplicate_entry, release, 'once'); others.append(duplicate)
        assert await finish(duplicate) == 'True'
        assert not duplicate_entry.exists() and not path.exists()
    finally:
        for process in [owner, *others]:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=15)
