# The Warp SQL site

The protocol's documentation site: the specification, every flow with a sequence diagram and
commands that run, the OpenAPI reference in Swagger UI, conformance and implementations. Built
with [Astro Starlight](https://starlight.astro.build); static output, no script needed except
for Swagger UI and search.

```bash
npm ci
npm run build && npm run preview      # http://localhost:4321, the private preview
DOCS_GA=true npm run build            # the public site, in dist/
npm run dev                           # live reload while writing
```

## Private preview and GA

The same source builds two sites, chosen by **one switch, `DOCS_GA`**:

| | Private preview (`DOCS_GA` unset) | Public site (`DOCS_GA=true`) |
|---|---|---|
| Address | <https://glowing-system-8l9q85e.pages.github.io/> (GitHub Pages, visible to the gizmodata organization only) | <https://warpsql.gizmodata.com/> (`public/CNAME`; DNS already points there) |
| Banner, `noindex` | on every page | none |
| External links | `github.com/gizmodata/` left out | checked |

Every run of `site.yml` builds both variants and `scripts/check_ga_build.py` fails it if the
public build still carries the banner or the `noindex` tag. A run started by hand with the **ga**
input checks the public site without flipping anything.

**The flip, when the repository is public:** set the repository variable `DOCS_GA` to `true`
(`gh variable set DOCS_GA --body true --repo gizmodata/warp-sql`), set the Pages custom domain
(the GA checklist below), and push to `main` (or run **site** by hand on `main`).

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

## GA checklist

Done, ready for the flip:

- [x] DNS: `warpsql.gizmodata.com` is a `CNAME` to `gizmodata.github.io`
      (gizmodata-infrastructure-terraform `aws/dns/gizmodata_com.tf`, applied 2026-10-20), and
      gizmodata.com is a verified GitHub Pages domain of the organization.
- [x] `public/CNAME`, the `DOCS_GA` switch, and the GA build checked on every run.

At the flip (the repository public):

- [ ] `gh variable set DOCS_GA --body true --repo gizmodata/warp-sql`.
- [ ] Pages custom domain: `gh api -X PUT repos/gizmodata/warp-sql/pages -f cname=warpsql.gizmodata.com`
      (Settings → Pages → Custom domain). Wait for the DNS check and the certificate
      (`gh api repos/gizmodata/warp-sql/pages --jq '.https_certificate.state'` says `approved`),
      then `gh api -X PUT repos/gizmodata/warp-sql/pages -F https_enforced=true`.
- [ ] A push to `main` that touches the site (or **site** by hand on `main`) deploys the public
      site; open every page on the live domain in Chromium and WebKit.
