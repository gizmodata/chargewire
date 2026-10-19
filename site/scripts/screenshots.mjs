// Screenshots of the built site in Chromium and WebKit, light and dark, desktop and phone,
// and checks of every page: no console errors, no horizontal scrolling at phone width, no
// request to another origin (the site and Swagger UI load nothing from a CDN), and Swagger
// UI showing every operation of the OpenAPI document.
//   npm run build && npx astro preview --port 4329 &  node scripts/screenshots.mjs [outdir]
import { chromium, webkit } from '@playwright/test';
import fs from 'node:fs';

const base = process.env.DOCS_PREVIEW_URL ?? 'http://localhost:4329';
const out = process.argv[2] ?? 'test-output/screenshots';
fs.mkdirSync(out, { recursive: true });
const shots = [
	{ name: 'home', path: '/' },
	{ name: 'flow-sign-in', path: '/flows/sign-in/', scrollTo: 'h2#browser-sign-in' },
	{ name: 'openapi', path: '/reference/openapi/', waitFor: '.swagger-ui .opblock' },
	{
		name: 'openapi-operation',
		path: '/reference/openapi/#/Statements/submitStatement',
		waitFor: '.swagger-ui .opblock',
		scrollTo: '#operations-Statements-submitStatement',
		click: '#operations-Statements-submitStatement .opblock-summary-control',
	},
	{ name: 'specification', path: '/specification/' },
];
const sizes = { desktop: { width: 1440, height: 1000 }, phone: { width: 390, height: 844 } };
const pages = fs
	.readFileSync('dist/sitemap-0.xml', 'utf8')
	.match(/<loc>[^<]+<\/loc>/g)
	.map((l) => new URL(l.slice(5, -6)).pathname);
const operations = (() => {
	const doc = JSON.parse(fs.readFileSync('public/openapi.json', 'utf8'));
	return Object.values(doc.paths).reduce(
		(n, item) => n + Object.keys(item).filter((m) => ['get', 'post', 'patch', 'delete', 'put'].includes(m)).length,
		0,
	);
})();
let problems = 0;
const origin = new URL(base).origin;
for (const [engineName, engine] of [['chromium', chromium], ['webkit', webkit]]) {
	const browser = await engine.launch();
	for (const scheme of ['light', 'dark']) {
		for (const [sizeName, viewport] of Object.entries(sizes)) {
			const context = await browser.newContext({ viewport, colorScheme: scheme, deviceScaleFactor: sizeName === 'phone' ? 2 : 1 });
			const page = await context.newPage();
			const errors = [];
			const foreign = new Set();
			let watching = true;
			page.on('console', (m) => watching && m.type() === 'error' && errors.push(m.text()));
			page.on('pageerror', (e) => watching && errors.push(String(e)));
			page.on('request', (r) => {
				const url = r.url();
				if (!url.startsWith(origin) && !url.startsWith('data:')) foreign.add(url);
			});
			for (const shot of shots) {
				await page.goto(base + shot.path, { waitUntil: 'networkidle' });
				if (shot.waitFor) {
					await page.waitForSelector(shot.waitFor, { timeout: 20000 });
					const shown = await page.locator('.swagger-ui .opblock').count();
					if (shown !== operations) {
						console.log(`${engineName} ${scheme} ${sizeName}: Swagger UI shows ${shown} of ${operations} operations`);
						problems++;
					}
				}
				if (shot.click) {
					const control = page.locator(shot.click);
					if ((await control.getAttribute('aria-expanded')) !== 'true') await control.click();
					await page.waitForTimeout(300);
				}
				if (shot.scrollTo) await page.locator(shot.scrollTo).evaluate((e) => e.scrollIntoView({ block: 'start' }));
				await page.waitForTimeout(300);
				await page.screenshot({ path: `${out}/${shot.name}-${engineName}-${scheme}-${sizeName}.png`, fullPage: false });
			}
			watching = false;
			if (sizeName === 'phone') {
				for (const path of pages) {
					await page.goto(base + path, { waitUntil: 'load' });
					const overflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
					if (overflow > 1) {
						console.log(`${engineName} ${scheme}: ${path} scrolls sideways by ${overflow}px`);
						problems++;
					}
				}
			}
			for (const e of errors) {
				console.log(`${engineName} ${scheme} ${sizeName}: console error: ${e}`);
				problems++;
			}
			for (const url of foreign) {
				console.log(`${engineName} ${scheme} ${sizeName}: a request to another origin: ${url}`);
				problems++;
			}
			await context.close();
		}
	}
	await browser.close();
}
console.log(`${problems} problems`);
process.exit(problems ? 1 : 0);
