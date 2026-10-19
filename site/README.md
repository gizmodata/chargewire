# The Warp SQL site

The protocol's documentation site: the specification, every flow with a sequence diagram and
commands that run, the OpenAPI reference in Swagger UI, conformance and implementations. Built
with [Astro Starlight](https://starlight.astro.build); static output, no script needed except
for Swagger UI and search. Its future address is `https://warpsql.gizmodata.com`; until the
public launch it is a private preview (every page says so and asks search engines not to index
it).

```bash
npm ci
npm run build && npm run preview      # http://localhost:4321
npm run dev                           # live reload while writing
```

## What is generated

The specification is the single source of truth. `npm run generate` (`scripts/generate.mjs`)
makes these from it, and `npm run check:generated` fails when a committed page is stale:

| Page | From |
|---|---|
| `src/content/docs/specification.md` | `spec/warp-sql.md`, every "§N.M" a link to its heading |
| `src/content/docs/reference/endpoints.md` | the spec's Appendix B and `spec/openapi.yaml` |
| `src/content/docs/conformance.md` | `conformance/README.md` and each test file's docstring |
| `src/data/spec-sections.json` | the spec's headings, for `<Spec s="6.1" />` links |
| `public/openapi.{yaml,json}` (not committed) | `spec/openapi.yaml`, made by every build |

Sequence diagrams are Mermaid sources in `src/diagrams/`, rendered by `npm run diagrams`
(`scripts/render_diagrams.mjs`, Mermaid in Playwright's Chromium) to a light and a dark SVG in
`src/assets/diagrams/`, which are committed: the build needs no browser and the pages no script.
`check:generated` also fails when an SVG is older than its source. Run
`npx playwright install chromium` once before rendering.

The OpenAPI reference (`src/pages/reference/openapi.astro`) is Swagger UI from
`swagger-ui-dist`, bundled with the site (no CDN, its online validator off).
`npm run lint:openapi` lints the document.

## Every example runs

A code block whose info string has the word `test` is an example `scripts/verify_examples.py`
runs against real servers; an `output` block right after it is what it must print (`…` matches
any text). Each page runs in a fresh directory and environment; its docstring lists the server
profiles. It needs a built reference server (`../cpp`, see the repository README), and
Hardcharger for the token-exchange example:

```bash
../conformance/.venv/bin/python scripts/verify_examples.py \
  --hardcharger /path/to/hardcharger-server        # or WARP_SQL_SITE_HARDCHARGER
```

`scripts/screenshots.mjs` checks the built site in Chromium and WebKit, light and dark, desktop
and phone: no console errors, no sideways scrolling on a phone, no request to another origin,
and Swagger UI showing every operation.

```bash
npx playwright install chromium webkit
npm run build && npx astro preview --port 4329 &
node scripts/screenshots.mjs test-output/screenshots
```

## CI

`.github/workflows/site.yml` runs on pull requests and pushes to `main` that touch the site, the
spec or the conformance suite's README and tests: the generated-page and diagram check, the
OpenAPI lint, the build (which fails on a broken internal link or anchor) and lychee on the
external links (`lychee.toml`). A push to `main` then deploys the site to GitHub Pages. The
examples and browser checks run locally (above): they need a built server.

## Launch checklist

1. Repository visibility: GitHub Pages for a private repository is visible only to the
   organization's members (the private preview). For the public site, the repository (or a
   public mirror of `site/`) must allow public Pages.
2. DNS: a `CNAME` record `warpsql.gizmodata.com` → `gizmodata.github.io`.
3. Pages: set the custom domain `warpsql.gizmodata.com` in the repository's Pages settings and
   enforce HTTPS once the certificate is issued.
4. Build: set `DOCS_SITE=https://warpsql.gizmodata.com` and `DOCS_INDEXABLE=true` in the
   `pages` job (drops the preview banner and the `noindex` tag).
5. `lychee.toml`: remove the exclusion of `github.com/gizmodata/` once the repositories are public.
