"""Check the resumable part-downloader in lakefs_util.io_util.

Run: PYTHONPATH=src python test_download_retry.py

Covers the property that matters over a WAN: a peer that truncates every
response still completes, because each retry resumes from the bytes already
written -- while a peer that sends nothing at all still fails.
"""
import asyncio
import os
import sys
import tempfile

import aiohttp

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
os.environ["LAKEFS_DOWNLOAD_STALLS"] = "3"

from lakefs_util import io_util  # noqa: E402

DATA = bytes((i * 7 + 11) % 256 for i in range(500_000))


class _Content:
    def __init__(self, payload, cut_after):
        self.payload, self.cut_after = payload, cut_after

    async def iter_chunked(self, n):
        sent = 0
        for i in range(0, len(self.payload), n):
            chunk = self.payload[i:i + n]
            if self.cut_after is not None and sent + len(chunk) > self.cut_after:
                chunk = chunk[: max(0, self.cut_after - sent)]
                if chunk:
                    yield chunk
                raise aiohttp.ClientPayloadError("Response payload is not completed")
            sent += len(chunk)
            yield chunk


class _Resp:
    def __init__(self, content):
        self.status, self.content = 206, content

    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


class FakeSession:
    """Serves Range requests, truncating each response after `cut` bytes."""

    def __init__(self, cut):
        self.cut, self.requests = cut, 0

    def get(self, url, headers=None):
        self.requests += 1
        start, end = (int(x) for x in headers["Range"].split("=")[1].split("-"))
        return _Resp(_Content(DATA[start:end + 1], self.cut))


async def _download(session, parts=2):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "obj.bin")
        await io_util.download_file("obj.bin", "repo", "main", path, session, parts=parts)
        return open(path, "rb").read()


async def main():
    io_util._stat_size = lambda *a, **k: _resolved(len(DATA))
    io_util.PARALLEL_THRESHOLD_BYTES = 1  # force the parallel path

    # A peer that always truncates at 64KiB: progress every attempt, so it finishes.
    chatty = FakeSession(cut=64 * 1024)
    got = await _download(chatty)
    assert got == DATA, f"corrupt download: {len(got)} bytes, expected {len(DATA)}"
    assert chatty.requests > 4, "expected several resumed range requests"

    # A peer that sends nothing: no progress, so it gives up after LAKEFS_DOWNLOAD_STALLS.
    dead = FakeSession(cut=0)
    try:
        await _download(dead)
    except aiohttp.ClientPayloadError:
        pass
    else:
        raise AssertionError("a zero-progress peer should not be retried forever")
    assert dead.requests <= 2 * (3 + 1), f"retried too many times: {dead.requests}"

    print(f"download retry checks passed ({chatty.requests} resumed requests to finish a truncating peer)")


def _resolved(v):
    f = asyncio.get_event_loop().create_future()
    f.set_result(v)
    return f


if __name__ == "__main__":
    asyncio.run(main())
