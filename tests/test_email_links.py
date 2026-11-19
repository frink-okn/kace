"""Check the links and switches in outgoing mail.

Run: PYTHONPATH=src python tests/test_email_links.py

Both notification emails point KG owners at infrastructure that moved during
the migration (frink.apps.renci.org -> apps.okn.us, the RENCI lakeFS ->
repository.okn.us), so the hosts must come from config rather than literals.

Two bugs this pins:
  * the deployed email put the KG *title* into ?sources=, producing
    "sources=DREAM-KG (QLever Endpoint)" -- spaces, parentheses, wrong value.
  * stop_email was a raw string, so STOP_EMAIL="false" is truthy and silently
    kept every notification off.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from canary.mail import mail_canary  # noqa: E402
from config import config  # noqa: E402


def links(html):
    return set(re.findall(r'href="([^"]+)"', html))


def main():
    # --- review email: every lakeFS link built from lakefs_public_url --------
    config.lakefs_public_url = "https://repository.okn.us/"
    repo, ver, branch = "dream-kg", "v0.0.5", "stable_v0_0_5"
    url = config.lakefs_public_url.rstrip("/") + "/repositories/" + repo
    review = mail_canary.review_email_template(url, repo, branch, ver, "", "")

    http = [h for h in links(review) if h.startswith("http")]
    assert http, "review email has no links"
    for h in http:
        assert h.startswith("https://repository.okn.us/repositories/dream-kg"), h
    # the steps must name the branch and the exact tag to create
    assert branch in review and f"<b>{ver}</b>" in review
    assert f"{url}/tags" in review, "no link to the tags page"

    # --- deployed email: slug, not title, and both links present ------------
    config.frink_address = "apps.okn.us"
    html = _render_deployed(title="DREAM-KG (QLever Endpoint)", slug="dreamkg", version=ver)
    hs = {h for h in links(html) if h.startswith("http")}
    assert "https://apps.okn.us/dreamkg/sparql" in html, "endpoint should be the /sparql form"
    src = [h for h in hs if "sources=" in h]
    assert src, "no query-page link"
    assert "sources=dreamkg" in src[0], src[0]
    for h in hs:
        assert " " not in h and "(" not in h, f"malformed URL: {h}"
        assert h.startswith("https://apps.okn.us"), h

    # a KG with no shortname must still produce a usable mail, not a bad link
    html = _render_deployed(title="Some KG", slug="", version=ver)
    assert not [h for h in links(html) if h.startswith("http") and h.endswith("/")]

    # --- the suppression switch is a boolean --------------------------------
    for raw, expected in [("", False), ("false", False), ("FALSE", False), ("no", False),
                          ("true", True), ("1", True), ("on", True)]:
        assert _parse_stop(raw) is expected, f"STOP_EMAIL={raw!r}"

    print("email link checks passed (lakeFS + query page from config, sources uses the slug)")


def _render_deployed(title, slug, version):
    """Mirror send_deployed_email's URL building, without sending anything."""
    import urllib.parse
    base = config.frink_address.rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = "https://" + base
    s = urllib.parse.quote(slug or "", safe="")
    access = f"{base}/?query=Q" + (f"&sources={s}" if s else "")
    endpoint = f"{base}/{s}/sparql" if s else None
    return mail_canary.deployed_email_template(title, version, access, endpoint)


def _parse_stop(raw):
    return raw.strip().lower() in ("1", "true", "yes", "on")


if __name__ == "__main__":
    main()
