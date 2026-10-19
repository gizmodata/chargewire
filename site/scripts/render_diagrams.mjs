// Renders every sequence diagram (src/diagrams/<name>.mmd) to a light and a dark SVG,
// src/assets/diagrams/<name>.{light,dark}.svg, with Mermaid in a headless Chromium. The SVGs
// are committed, so building the site needs no browser and its pages no script.
//
//   node scripts/render_diagrams.mjs            render what changed
//   node scripts/render_diagrams.mjs --all      render everything again
//   node scripts/render_diagrams.mjs --check    fail if an SVG is missing or older than its source
//
// Each SVG records the SHA-256 of what it was made from (the source, the Mermaid version and
// this script's theme) in a data-source-sha256 attribute: --check compares that, not the SVG
// itself, whose text positions depend on the fonts of the machine that rendered it.
import { chromium } from '@playwright/test';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sources = path.join(root, 'src/diagrams');
const out = path.join(root, 'src/assets/diagrams');
const mermaidPath = path.join(root, 'node_modules/mermaid/dist/mermaid.min.js');
const mermaidVersion = JSON.parse(fs.readFileSync(path.join(root, 'node_modules/mermaid/package.json'), 'utf8')).version;

// Fonts every reader has: the SVGs are images, which can't load web fonts.
const fontFamily = "Helvetica, Arial, 'Liberation Sans', sans-serif";
const themes = {
	light: {
		background: '#ffffff',
		primaryColor: '#eaf6fb',
		primaryBorderColor: '#31a8db',
		primaryTextColor: '#0b1416',
		actorBkg: '#eaf6fb',
		actorBorder: '#31a8db',
		actorTextColor: '#0b1416',
		actorLineColor: '#95a9a6',
		signalColor: '#2a3a3d',
		signalTextColor: '#0b1416',
		labelBoxBkgColor: '#fff1ea',
		labelBoxBorderColor: '#f15a24',
		labelTextColor: '#0b1416',
		loopTextColor: '#0b1416',
		noteBkgColor: '#fff1ea',
		noteBorderColor: '#f15a24',
		noteTextColor: '#0b1416',
		activationBkgColor: '#d6eef8',
		activationBorderColor: '#31a8db',
		sequenceNumberColor: '#ffffff',
	},
	dark: {
		background: '#0b1416',
		primaryColor: '#142328',
		primaryBorderColor: '#31a8db',
		primaryTextColor: '#e4ecea',
		actorBkg: '#142328',
		actorBorder: '#31a8db',
		actorTextColor: '#e4ecea',
		actorLineColor: '#5b6f6c',
		signalColor: '#c9d6d4',
		signalTextColor: '#e4ecea',
		labelBoxBkgColor: '#2a1810',
		labelBoxBorderColor: '#f15a24',
		labelTextColor: '#f5f8f7',
		loopTextColor: '#e4ecea',
		noteBkgColor: '#2a1810',
		noteBorderColor: '#f15a24',
		noteTextColor: '#f5f8f7',
		activationBkgColor: '#1f3a44',
		activationBorderColor: '#31a8db',
		sequenceNumberColor: '#0b1416',
	},
};
const sequence = { mirrorActors: false, messageAlign: 'left', boxMargin: 8, noteMargin: 8, messageMargin: 32, useMaxWidth: false, wrap: false };
const recipe = JSON.stringify({ mermaidVersion, fontFamily, themes, sequence, version: 1 });

function digest(source) {
	return crypto.createHash('sha256').update(recipe).update('\0').update(source).digest('hex');
}

function recorded(file) {
	if (!fs.existsSync(file)) return null;
	const match = fs.readFileSync(file, 'utf8').match(/data-source-sha256="([0-9a-f]{64})"/);
	return match ? match[1] : null;
}

const args = new Set(process.argv.slice(2));
const names = fs.readdirSync(sources).filter((f) => f.endsWith('.mmd')).map((f) => f.slice(0, -4)).sort();
const stale = [];
for (const name of names) {
	const want = digest(fs.readFileSync(path.join(sources, `${name}.mmd`), 'utf8'));
	for (const theme of Object.keys(themes)) {
		if (args.has('--all') || recorded(path.join(out, `${name}.${theme}.svg`)) !== want) stale.push({ name, theme, want });
	}
}
const orphans = fs.existsSync(out)
	? fs.readdirSync(out).filter((f) => f.endsWith('.svg') && !names.includes(f.replace(/\.(light|dark)\.svg$/, '')))
	: [];

if (args.has('--check')) {
	for (const s of stale) console.log(`stale: src/assets/diagrams/${s.name}.${s.theme}.svg (from src/diagrams/${s.name}.mmd)`);
	for (const o of orphans) console.log(`orphan: src/assets/diagrams/${o} has no source`);
	if (stale.length || orphans.length) {
		console.log('Run: npm run diagrams, and commit src/assets/diagrams.');
		process.exit(1);
	}
	console.log(`diagrams: ${names.length} sources, every SVG current`);
	process.exit(0);
}

fs.mkdirSync(out, { recursive: true });
for (const o of orphans) fs.rmSync(path.join(out, o));
if (stale.length) {
	const browser = await chromium.launch();
	const page = await browser.newPage();
	await page.setContent('<!doctype html><html><body></body></html>');
	await page.addScriptTag({ path: mermaidPath });
	for (const { name, theme, want } of stale) {
		const source = fs.readFileSync(path.join(sources, `${name}.mmd`), 'utf8');
		let svg = await page.evaluate(
			async ({ source, theme, themeVariables, sequence, fontFamily, id }) => {
				// eslint-disable-next-line no-undef
				mermaid.initialize({ startOnLoad: false, theme: 'base', themeVariables: { ...themeVariables, fontFamily }, sequence, fontFamily, securityLevel: 'strict' });
				// eslint-disable-next-line no-undef
				const { svg } = await mermaid.render(id, source);
				return svg;
			},
			{ source, theme, themeVariables: themes[theme], sequence, fontFamily, id: `d-${name}-${theme}` },
		);
		// An image needs its own size: Mermaid writes width="100%" and a max-width style.
		const box = svg.match(/viewBox="([-\d.]+) ([-\d.]+) ([\d.]+) ([\d.]+)"/);
		if (!box) throw new Error(`${name}: Mermaid's SVG has no viewBox`);
		const [w, h] = [Math.ceil(Number(box[3])), Math.ceil(Number(box[4]))];
		svg = svg.replace(/<svg([^>]*?)\swidth="[^"]*"/, `<svg$1 width="${w}"`).replace(/\sstyle="max-width:[^"]*"/, '');
		if (!/<svg[^>]*\sheight="/.test(svg)) svg = svg.replace('<svg', `<svg height="${h}"`);
		svg = svg.replace('<svg', `<svg data-source-sha256="${want}"`);
		fs.writeFileSync(path.join(out, `${name}.${theme}.svg`), svg + '\n');
		console.log(`rendered ${name}.${theme}.svg (${w}x${h})`);
	}
	await browser.close();
}
console.log(`diagrams: ${names.length} sources, ${stale.length} SVGs rendered`);
