#!/usr/bin/env python3
"""Check a build of the public site (DOCS_GA=true): nothing of the private preview is left in it.

    python3 scripts/check_ga_build.py dist

Fails if any page still carries the preview banner or the noindex tag; if dist/CNAME doesn't
name the custom domain; or if the sitemap isn't on it. CI runs it on a DOCS_GA=true build of
every run, so the switch is proven before it is flipped (site/README.md, "Private preview and
GA").
"""
import re
import sys
from pathlib import Path

DOMAIN = "warpsql.gizmodata.com"
FORBIDDEN = [
    re.compile(p, re.I) for p in (
        r"private preview",
        r"internal to gizmodata",
        r"<meta[^>]*name=\"robots\"[^>]*noindex",
    )
]


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    pages = sorted(p for p in out.rglob("*.html") if "pagefind" not in p.parts)
    if not pages:
        print(f"{out}: no HTML pages: not a build", file=sys.stderr)
        return 1
    bad: list[str] = []
    for page in pages:
        text = page.read_text(encoding="utf-8", errors="replace")
        for pattern in FORBIDDEN:
            for m in pattern.finditer(text):
                snippet = text[max(0, m.start() - 60):m.end() + 60].replace("\n", " ")
                bad.append(f"{page.relative_to(out)}: /{pattern.pattern}/: …{snippet}…")
                break
    cname = out / "CNAME"
    if not cname.is_file() or cname.read_text(encoding="utf-8").strip() != DOMAIN:
        bad.append(f"{cname}: must contain exactly {DOMAIN}")
    sitemap = out / "sitemap-index.xml"
    if not sitemap.is_file() or f"https://{DOMAIN}/" not in sitemap.read_text(encoding="utf-8"):
        bad.append(f"{sitemap}: must point at https://{DOMAIN}/ (build with DOCS_SITE=https://{DOMAIN})")
    if bad:
        print(f"{out}: {len(bad)} problem(s) for the public site:")
        print("\n".join(bad))
        return 1
    print(f"{out}: {len(pages)} pages, nothing of the private preview, CNAME and sitemap on {DOMAIN}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
