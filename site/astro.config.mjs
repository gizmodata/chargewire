// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';
import starlightLlmsTxt from 'starlight-llms-txt';

// The site's address. DOCS_SITE and DOCS_BASE point a build at wherever it is served (the
// custom domain, the private preview on GitHub Pages, localhost). DOCS_GA=true is the one
// switch between the private preview and the public site (README, "Private preview and GA"):
// without it every page says noindex and carries the preview banner.
const site = process.env.DOCS_SITE ?? 'https://warpsql.gizmodata.com';
const base = process.env.DOCS_BASE ?? '/';
const indexable = process.env.DOCS_GA === 'true';

export default defineConfig({
	site,
	base,
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'Warp SQL',
			description:
				'Warp SQL is an open protocol for running SQL over plain HTTP, with Apache Arrow IPC streams for results, parameters and uploads.',
			logo: { src: './src/assets/warp-sql-mark.svg', alt: '' },
			favicon: '/favicon.svg',
			customCss: ['./src/styles/theme.css'],
			editLink: { baseUrl: 'https://github.com/gizmodata/warp-sql/edit/main/site/' },
			lastUpdated: true,
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/gizmodata/warp-sql' }],
			components: {
				Footer: './src/components/Footer.astro',
				Banner: './src/components/Banner.astro',
				PageTitle: './src/components/PageTitle.astro',
			},
			head: [
				{ tag: 'meta', attrs: { name: 'theme-color', content: '#0b1416' } },
				...(indexable ? [] : [{ tag: 'meta', attrs: { name: 'robots', content: 'noindex, nofollow' } }]),
			],
			expressiveCode: {
				themes: ['github-dark-default', 'github-light-default'],
				styleOverrides: { borderRadius: '0.6rem', frames: { shadowColor: 'transparent' } },
			},
			plugins: [
				// The OpenAPI reference is an Astro page of its own (src/pages), which the
				// validator doesn't know: links to it are excluded and checked by the build's
				// own page list instead (scripts/generate.mjs --check-links).
				starlightLinksValidator({ errorOnLocalLinks: false, exclude: ['/reference/openapi/', '/reference/openapi/**', '/openapi.yaml', '/openapi.json'] }),
				starlightLlmsTxt({ projectName: 'Warp SQL' }),
			],
			sidebar: [
				{ label: 'Overview', link: '/' },
				{ label: 'Specification', slug: 'specification' },
				{
					label: 'Flows',
					items: [
						{ label: 'Sign-in', slug: 'flows/sign-in' },
						{ label: 'Sessions and transactions', slug: 'flows/sessions' },
						{ label: 'A query in one round trip', slug: 'flows/streamed-query' },
						{ label: 'Submit, long-poll, fetch', slug: 'flows/async-query' },
						{ label: 'Retention and resumption', slug: 'flows/retention' },
						{ label: 'Partitioned results', slug: 'flows/partitioned-results' },
						{ label: 'Prepared statements', slug: 'flows/prepared-statements' },
						{ label: 'Bulk ingest', slug: 'flows/bulk-ingest' },
						{ label: 'Cancel and close', slug: 'flows/cancel-and-close' },
						{ label: 'Errors mid-stream', slug: 'flows/errors-mid-stream' },
						{ label: 'Graceful shutdown', slug: 'flows/graceful-shutdown' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'OpenAPI (Swagger UI)', link: '/reference/openapi/' },
						{ label: 'Endpoints', slug: 'reference/endpoints' },
					],
				},
				{ label: 'Conformance', slug: 'conformance' },
				{ label: 'Implementations', slug: 'implementations' },
				{ label: 'Versioning and compatibility', slug: 'versioning' },
			],
		}),
	],
});
