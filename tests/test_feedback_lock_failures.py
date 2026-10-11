"""Resource fault injection and subprocess event-loop liveness under flock contention."""
import asyncio
import fcntl
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from test_deferred_questions import CORE, PROJECT, ControlledAdapter, runtime
from test_answer_feedback import answered
from test_feedback_lock_cleanup import finish, wait_entered


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
@pytest.mark.parametrize('fault', ['read', 'admission-unlock', 'cleanup-unlock',
                                  'question-unlock', 'guard-close', 'question-close', 'all-release'])
async def test_cleanup_faults_preserve_result_and_release_all_fds(installed, monkeypatch, cancel, fault):
    rt = runtime(installed[0]); qid = answered(rt)
    entered = asyncio.Event()
    async def edit(**kwargs):
        entered.set()
        if cancel:
            await asyncio.Event().wait()
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=AsyncMock(side_effect=edit))),
                               ControlledAdapter.make('telegram'))
    opened = []
    original_open, original_flock = Path.open, fcntl.flock
    class FD:
        def __init__(self, stream, kind):
            self.stream, self.kind = stream, kind
            self.closes = self.unlocks = 0
        def fileno(self):
            return self.stream.fileno()
        def close(self):
            self.closes += 1
            self.stream.close()
            if fault in (self.kind + '-close', 'all-release'):
                raise OSError('injected close failure')
    def open_lock(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path.name == 'feedback.lock' or path.name == f'feedback-{qid}.lock':
            fd = FD(stream, 'guard' if path.name == 'feedback.lock' else 'question')
            opened.append(fd)
            return fd
        return stream
    def flock(fd, operation):
        if isinstance(fd, FD) and operation == fcntl.LOCK_UN:
            fd.unlocks += 1
            stage = 'admission-unlock' if fd is opened[0] else 'cleanup-unlock'
            if fault == 'all-release' or fault == stage and fd.kind == 'guard' or (
                    fault == 'question-unlock' and fd.kind == 'question'):
                raise OSError('injected unlock failure')
        return original_flock(fd, operation)
    feedback = rt.store.feedback
    def read(question_id):
        if fault == 'read' and len(opened) == 3:
            raise OSError('injected cleanup read failure')
        return feedback(question_id)
    monkeypatch.setattr(Path, 'open', open_lock)
    monkeypatch.setattr(fcntl, 'flock', flock)
    monkeypatch.setattr(rt.store, 'feedback', read)
    task = asyncio.create_task(rt.answer_feedback(qid))
    await asyncio.wait_for(entered.wait(), 5)
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    else:
        assert await asyncio.wait_for(task, 5)
    assert len(opened) == 3
    assert all(fd.closes == 1 and fd.unlocks == 1 and fd.stream.closed for fd in opened)
    assert feedback(qid)['state'] == ('pending' if cancel else 'done')
    if cancel or fault == 'read':
        assert rt.store.path.with_name(f'feedback-{qid}.lock').exists()


@pytest.mark.asyncio
async def test_question_acquisition_error_closes_both_fds(installed, monkeypatch):
    rt = runtime(installed[0]); qid = answered(rt)
    opened = []
    original_open, original_flock = Path.open, fcntl.flock
    def open_lock(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path.name.startswith('feedback') and path.suffix == '.lock':
            opened.append(stream)
        return stream
    def flock(fd, operation):
        if len(opened) == 2 and fd is opened[1] and operation != fcntl.LOCK_UN:
            raise OSError('injected acquisition failure')
        return original_flock(fd, operation)
    monkeypatch.setattr(Path, 'open', open_lock)
    monkeypatch.setattr(fcntl, 'flock', flock)
    with pytest.raises(OSError, match='acquisition'):
        await rt.answer_feedback(qid)
    assert len(opened) == 2 and all(fd.closed for fd in opened)


RESPONSIVE_CHILD = '''
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
async def main():
    ticks = []
    async def heartbeat():
        while True:
            ticks.append(1)
            await asyncio.sleep(.01)
    beat = asyncio.create_task(heartbeat())
    async def edit(**kwargs):
        Path(entered).write_text('question lock acquired')
        while not Path(release).exists():
            await asyncio.sleep(.01)
        if mode != 'cleanup-success':
            await asyncio.Event().wait()
    native = NS(bot=NS(edit_message_text=AsyncMock(side_effect=edit)))
    adapter = ControlledAdapter.make('telegram')
    rt.adapters['telegram'] = (native, adapter)
    # Real manager.unload -> Runtime.close -> lifecycle.drain owns this task.
    rt._bindings['telegram'] = (native, adapter, asyncio.get_running_loop(), [])
    task = rt._spawn(rt.answer_feedback(qid), 'telegram')
    if mode.startswith('admission'):
        assert await asyncio.wait_for(task, 1) is False
        assert not rt.store.path.with_name('feedback-' + qid + '.lock').exists()
        # Busy admission must not prevent cancellation/drain of other owned work.
        task = rt._spawn(asyncio.Event().wait(), 'telegram')
    else:
        while not Path(release).exists():
            await asyncio.sleep(.01)
    await asyncio.sleep(.06)
    before = len(ticks)
    if mode.endswith('unload'):
        manager.unload()
        assert await asyncio.wait_for(asyncio.wrap_future(rt._drains['telegram']), 1)
    elif mode.endswith('cancel'):
        task.cancel()
    if mode == 'cleanup-success':
        assert await asyncio.wait_for(task, 1)
    else:
        try:
            await asyncio.wait_for(task, 1)
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError('cancellation lost')
    if not mode.startswith('admission'):
        assert rt.store.path.with_name('feedback-' + qid + '.lock').exists()
    await asyncio.sleep(.06)
    assert before >= 3 and len(ticks) > before + 2
    beat.cancel()
    try:
        await beat
    except asyncio.CancelledError:
        pass
    manager.unload()
    print('responsive', flush=True)
asyncio.run(main())
'''


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['admission-cancel', 'admission-unload', 'cleanup-success',
                                  'cleanup-cancel', 'cleanup-unload'])
async def test_cross_process_guard_busy_remains_responsive(installed, tmp_path, mode):
    rt = runtime(installed[0]); qid = answered(rt)
    entered, release = tmp_path / 'entered', tmp_path / 'release'
    guard = rt.store.path.with_name('feedback.lock').open('a')
    env = dict(os.environ, HOME=str(installed[2].parent), HERMES_HOME=str(installed[2]),
               HERMES_CORE_ROOT=str(CORE),
               PYTHONPATH=os.pathsep.join((str(PROJECT / 'tests'), str(CORE))))
    if mode.startswith('admission'):
        fcntl.flock(guard, fcntl.LOCK_EX)
    process = subprocess.Popen([sys.executable, '-c', RESPONSIVE_CHILD, qid, str(entered),
                               str(release), mode], cwd=installed[2], env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        if mode.startswith('cleanup'):
            await wait_entered(process, entered)
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
            release.write_text('guard remains held while child finishes')
        # Parent timeout can kill a child even if its event loop blocks in flock.
        assert await finish(process) == 'responsive'
        assert rt.store.feedback(qid)['state'] == ('done' if mode == 'cleanup-success' else 'pending')
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=15)
        guard.close()
    # Retained pending/done question paths remain usable after contention ends.
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=AsyncMock())), ControlledAdapter.make('telegram'))
    assert await rt.answer_feedback(qid)
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()
