"""Check the lakeFS upload path in lakefs_util.io_util.

Run: PYTHONPATH=src python test_upload_retry.py

The babel run died here: aiohttp's default ClientTimeout is 5 minutes TOTAL,
which no multi-GB cross-WAN upload can meet, and a failure had no retry. This
covers the three properties that fix needs: no total timeout, chunks read off
the event loop, and a transport failure restarting the file rather than failing
the workflow.
"""
import asyncio
import os
import sys
import tempfile

import aiohttp

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
os.environ["LAKEFS_UPLOAD_ATTEMPTS"] = "3"

from lakefs_util import io_util  # noqa: E402


class _Resp:
    def __init__(self, status): self.status = status
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def text(self): return "error"


class FakeSession:
    """Consumes the body generator; fails while the shared budget says to.

    The counter is shared across instances on purpose: the retry loop builds a
    fresh ClientSession per attempt, so a per-instance counter would never run
    out and the test would never reach the success path.
    """

    remaining_failures = 0

    def __init__(self):
        self.attempts, self.bytes_last, self.timeout = 0, 0, None

    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False

    def post(self, url, data=None):
        self.attempts += 1
        should_fail = FakeSession.remaining_failures > 0
        if should_fail:
            FakeSession.remaining_failures -= 1
        sess = self

        class _Ctx:
            async def __aenter__(self):
                sent = 0
                async for chunk in data:
                    sent += len(chunk)
                    if should_fail and sent > 1024:
                        raise aiohttp.ClientError("connection reset")
                sess.bytes_last = sent
                return _Resp(201)
            async def __aexit__(self, *a): return False
        return _Ctx()


async def main():
    payload = os.urandom(300_000)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "graph.hdt")
        open(path, "wb").write(payload)

        sessions = []
        def fake_session(*a, **kw):
            s = FakeSession()
            s.timeout = kw.get("timeout")
            sessions.append(s)
            return s

        io_util.aiohttp.ClientSession = fake_session
        io_util.login_and_get_cookies = lambda *a, **k: _done({})
        io_util.resolve_future_tag = lambda repo: _done("v0.0.1")
        io_util.lakefs.client.LakeFSClient = lambda **kw: _Stub()

        # 1. a clean upload sends every byte, with NO total timeout set
        await io_util.upload_files("repo", "main", [(path, "hdt")])
        assert sessions[-1].bytes_last == len(payload), "did not send the whole file"
        assert sessions[-1].timeout.total is None, "a total timeout would cap large uploads"
        assert sessions[-1].timeout.sock_read == io_util.UPLOAD_SOCK_READ_SECS

        # 2. a mid-stream failure restarts that file and still completes
        sessions.clear()
        FakeSession.remaining_failures = 2
        await io_util.upload_files("repo", "main", [(path, "hdt")])
        assert len(sessions) == 3, f"expected 2 retries then success, got {len(sessions)} attempts"
        assert sessions[-1].bytes_last == len(payload)

        # 3. exhausting the budget surfaces the error rather than hanging
        sessions.clear()
        FakeSession.remaining_failures = 99
        try:
            await io_util.upload_files("repo", "main", [(path, "hdt")])
        except aiohttp.ClientError:
            pass
        else:
            raise AssertionError("a permanently failing upload must raise")
        assert len(sessions) == io_util.UPLOAD_MAX_ATTEMPTS

    print(f"upload checks passed (no total timeout, {io_util.UPLOAD_MAX_ATTEMPTS}-attempt budget, whole file re-sent)")


def _done(v):
    f = asyncio.get_event_loop().create_future()
    f.set_result(v)
    return f


class _Stub:
    class _Api:
        def create_branch(self, **kw): pass
        def commit(self, **kw): pass
    branches_api = commits_api = _Api()


if __name__ == "__main__":
    asyncio.run(main())
