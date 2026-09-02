import * as fs from 'fs';
import * as path from 'path';

const DOCS_DIR = path.resolve(process.cwd(), '../docs');

function getFiles(dir, exts = ['.html', '.css', '.js']) {
  let files = [];
  const items = fs.readdirSync(dir, { withFileTypes: true });
  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      files = files.concat(getFiles(fullPath, exts));
    } else if (exts.includes(path.extname(item.name))) {
      files.push(fullPath);
    }
  }
  return files;
}

function relativizeHtml(filePath, content) {
  const relToDocs = path.relative(path.dirname(filePath), DOCS_DIR);
  const prefix = relToDocs === '' ? '.' : relToDocs.replace(/\\/g, '/');

  // Replace attributes with root-relative paths: href="/...", src="/...", etc.
  let updated = content.replace(/(href|src|data-href|data-site-url)=["']\/([^"']*)["']/g, (match, attr, target) => {
    // Leave protocol-relative (e.g. //cdn...) untouched if any
    if (target.startsWith('/')) return match;
    
    if (target === '') {
      return `${attr}="${prefix}/index.html"`;
    }
    
    // If target is a directory path ending with / or without extension
    let resolved = `${prefix}/${target}`;
    if (target.endsWith('/')) {
      resolved = `${prefix}/${target}index.html`;
    } else if (!path.extname(target) && !target.includes('#') && !target.includes('?')) {
      resolved = `${prefix}/${target}/index.html`;
    }
    return `${attr}="${resolved}"`;
  });

  // Also replace canonical / sitemap root links if needed
  updated = updated.replace(/href="\/sitemap-index\.xml"/g, `href="${prefix}/sitemap-index.xml"`);
  updated = updated.replace(/href="\/favicon\.svg"/g, `href="${prefix}/favicon.svg"`);

  return updated;
}

function relativizeCss(filePath, content) {
  const relToDocs = path.relative(path.dirname(filePath), DOCS_DIR);
  const prefix = relToDocs === '' ? '.' : relToDocs.replace(/\\/g, '/');

  return content.replace(/url\(["']?\/([^"')]+)["']?\)/g, (match, target) => {
    return `url("${prefix}/${target}")`;
  });
}

function relativizeJs(filePath, content) {
  const relToDocs = path.relative(path.dirname(filePath), DOCS_DIR);
  const prefix = relToDocs === '' ? '.' : relToDocs.replace(/\\/g, '/');

  // Replace "/_astro/..." in JS imports/assets
  let updated = content.replace(/(["'])\/_astro\//g, `$1${prefix}/_astro/`);
  updated = updated.replace(/(["'])\/pagefind\//g, `$1${prefix}/pagefind/`);
  return updated;
}

export function runRelativize() {
  if (!fs.existsSync(DOCS_DIR)) {
    console.error(`Docs directory not found at: ${DOCS_DIR}`);
    return;
  }

  const allFiles = getFiles(DOCS_DIR);
  console.log(`Relativizing ${allFiles.length} files in docs/...`);

  for (const file of allFiles) {
    const ext = path.extname(file);
    const content = fs.readFileSync(file, 'utf8');
    let updated = content;

    if (ext === '.html') {
      updated = relativizeHtml(file, content);
    } else if (ext === '.css') {
      updated = relativizeCss(file, content);
    } else if (ext === '.js') {
      updated = relativizeJs(file, content);
    }

    if (updated !== content) {
      fs.writeFileSync(file, updated, 'utf8');
    }
  }

  console.log('Successfully transformed all docs paths to relative ./ paths.');
}

if (import.meta.url.endsWith(process.argv[1])) {
  runRelativize();
}
