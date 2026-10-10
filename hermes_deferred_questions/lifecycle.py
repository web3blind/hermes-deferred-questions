"""Bounded passive connection receipts for conventional entrypoint imports.

No context/runtime/task is reachable from this cache. Fresh successful registration
alone owns recovery; failed drains are sticky. There are no cached supervisors.
"""
from collections import OrderedDict
from concurrent.futures import Future
from threading import RLock
import weakref

LOCK = RLock()
CACHE = OrderedDict()
LIMIT = 32
DRAIN_TIMEOUT = 10.0


def remember(home, platform, native, adapter, loop, receipt=None):
    key = (home, platform, id(adapter))
    try:
        refs = (weakref.ref(native) if native is not None else lambda: None,
                weakref.ref(adapter), weakref.ref(loop))
    except TypeError:
        return None  # Unsupported non-weakref native test doubles are not retained.
    with LOCK:
        prune()
        if key not in CACHE and len(CACHE) >= LIMIT:
            raise RuntimeError('binding_cache_full')
        CACHE[key] = (*refs, receipt)
        CACHE.move_to_end(key)
    return key


def prune():
    for key, (native, adapter, loop, receipt) in tuple(CACHE.items()):
        if adapter() is None or loop() is None or loop().is_closed():
            del CACHE[key]


def connections(home):
    with LOCK:
        prune()
        # Only the latest desired native connection may recover a platform.
        latest = {}
        for key, value in CACHE.items():
            if key[0] == home:
                latest[key[1]] = (key[1], *value)
        return list(latest.values())


def barrier(home, platform, adapter):
    with LOCK:
        # Ownership is platform-scoped, not restricted to the candidate identity.
        # A fresh adapter cannot bypass an older unfinished or failed drain.
        pending = None
        for key, value in CACHE.items():
            if key[:2] != (home, platform) or value[3] is None:
                continue
            receipt = value[3]
            if receipt.done():
                if not receipt.result():
                    return receipt  # Sticky failure wins over later completion.
            else:
                pending = receipt
        if pending is not None:
            return pending
        value = CACHE.get((home, platform, id(adapter)))
        return value[3] if value else None


def drain(loop, tasks, cleanups, timeout=None, prerequisites=()):
    """Passive token: cleanup AND all tasks AND prior-generation drains.

    Completion bookkeeping is finite and loop-owned, not cached. Tokens have no
    callbacks/subscribers: they cannot retain waiting tasks, contexts or runtimes.
    Task callbacks and bounded completion timers point TO the token, never back.
    """
    import asyncio
    timeout = DRAIN_TIMEOUT if timeout is None else timeout
    receipt = Future()
    pending = set(tasks)
    prerequisites = tuple(p for p in prerequisites if p is not None)
    def on_loop():
        ok = True
        for cleanup in reversed(cleanups):
            try:
                cleanup()
            except Exception:
                ok = False
        deadline = loop.time() + timeout
        timer = None
        def settle(task=None):
            nonlocal timer
            if task is not None:
                pending.discard(task)
                if not task.cancelled():
                    task.exception()
            if receipt.done():
                return
            if timer is not None:
                timer.cancel()
            prior_done = all(p.done() for p in prerequisites)
            if not pending and prior_done:
                receipt.set_result(ok and all(p.result() for p in prerequisites))
            elif loop.time() >= deadline:
                receipt.set_result(False)
            else:
                # This bounded cleanup completion check owns no plugin effects.
                timer = loop.call_later(min(.01, max(0, deadline - loop.time())), settle)
        for task in tuple(pending):
            if task.done():
                settle(task)
            else:
                task.add_done_callback(settle)
                task.cancel()
        settle()
    if loop.is_closed() or not loop.is_running():
        receipt.set_result(False)
    else:
        try:
            owning_thread = asyncio.get_running_loop() is loop
        except RuntimeError:
            owning_thread = False
        if owning_thread:
            on_loop()
        else:
            loop.call_soon_threadsafe(on_loop)
    return receipt
