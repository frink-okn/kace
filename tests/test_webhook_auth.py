"""Check the webhook shared-secret gate. Run: PYTHONPATH=src python test_webhook_auth.py

Only covers require_token — the one security decision in temporal_server.py.
"""
from fastapi import HTTPException

from config import config
from temporal_server import require_token


def _rejects(token, expect_status, expect_detail=None):
    try:
        require_token(token)
    except HTTPException as e:
        assert e.status_code == expect_status, f"got {e.status_code}, want {expect_status}"
        if expect_detail:
            assert expect_detail in e.detail, f"detail {e.detail!r} lacks {expect_detail!r}"
        return
    raise AssertionError(f"token {token!r} was accepted but should have failed")


if __name__ == "__main__":
    config.webhook_token = ""
    _rejects("anything", 503)          # unconfigured = fail closed, not fail open
    _rejects(None, 503)

    config.webhook_token = "s3cret"
    # absent and wrong must be distinguishable -- the caller reads this detail
    # out of its own run log when debugging a templated token.
    _rejects(None, 401, "missing")     # header absent entirely
    _rejects("", 401, "empty")         # present but substitution produced nothing
    _rejects("s3cre", 401, "invalid")  # prefix must not pass
    _rejects("s3cret ", 401, "invalid")  # no trimming
    _rejects("{{ ENV.SOME_TOKEN }}", 401, "invalid")  # unsubstituted template
    require_token("s3cret")            # exact match accepted

    print("webhook auth checks passed")
