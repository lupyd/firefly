// One-time setup for the example bots. Run from anywhere:  bun examples/bots-setup.ts
//
// 1. Builds the WebAssembly client (crates/client-node) with wasm-pack
// 2. Installs JS dependencies and compiles client-node and firefly-client-js
// 3. Links the packages together so every bot uses the code just built
// 4. Creates each bot's .env from .env.example
//
// Flags: --skip-wasm  reuse an existing wasm build (only re-run the TypeScript steps)
import { existsSync, copyFileSync, lstatSync, mkdirSync, rmSync, symlinkSync, unlinkSync } from 'fs';
import { join, resolve } from 'path';

const root = resolve(import.meta.dir, '..');
const clientNode = join(root, 'crates', 'client-node');
const framework = join(root, 'crates', 'firefly-client-js');
const bots = ['greetings-bot', 'reminder-bot'];
const frameworkUsers = [...bots, 'bots-e2e', 'chatbot'];
const isWindows = process.platform === 'win32';
const skipWasm = process.argv.includes('--skip-wasm');

function step(title: string): void {
  console.log(`\n==> ${title}`);
}

function fail(message: string): never {
  console.error(`\nSetup failed: ${message}`);
  process.exit(1);
}

function run(cmd: string[], cwd: string, env: Record<string, string> = {}): void {
  console.log(`$ ${cmd.join(' ')}`);
  const result = Bun.spawnSync(cmd, { cwd, env: { ...process.env, ...env }, stdout: 'inherit', stderr: 'inherit' });
  if (result.exitCode !== 0) fail(`"${cmd.join(' ')}" exited with code ${result.exitCode}`);
}

function has(command: string): boolean {
  return Bun.which(command) !== null;
}

// Replaces whatever is at `path` (a copied package, an old link) with a link to `target`.
// Junctions on Windows need no admin rights; elsewhere a normal directory symlink is used.
function link(path: string, target: string): void {
  mkdirSync(resolve(path, '..'), { recursive: true });
  if (existsSync(path) || isLink(path)) {
    if (lstatSync(path).isSymbolicLink()) unlinkSync(path);
    else rmSync(path, { recursive: true, force: true });
  }
  symlinkSync(target, path, isWindows ? 'junction' : 'dir');
}

function isLink(path: string): boolean {
  try {
    return lstatSync(path).isSymbolicLink();
  } catch {
    return false;
  }
}

function wasmToolchainEnv(): Record<string, string> {
  const llvm = 'C:/Program Files/LLVM/bin';
  if (isWindows && existsSync(`${llvm}/clang.exe`)) {
    return { CC_wasm32_unknown_unknown: `${llvm}/clang.exe`, AR_wasm32_unknown_unknown: `${llvm}/llvm-ar.exe` };
  }
  if (!has('clang')) {
    fail(isWindows
      ? 'clang not found. Install LLVM:  winget install LLVM.LLVM'
      : 'clang not found. Install it (e.g. apt install clang / brew install llvm).');
  }
  return {};
}

step('Checking tools');
for (const tool of ['bun', 'node', ...(skipWasm ? [] : ['cargo', 'wasm-pack'])]) {
  if (!has(tool)) {
    fail(tool === 'wasm-pack' ? 'wasm-pack not found. Install it:  cargo install wasm-pack' : `${tool} not found on PATH.`);
  }
  console.log(`ok  ${tool}`);
}

if (skipWasm) {
  step('Skipping the WebAssembly build (--skip-wasm)');
  if (!existsSync(join(clientNode, 'wasm', 'firefly_client_node.js'))) fail('No existing wasm build. Run without --skip-wasm.');
} else {
  step('Building the WebAssembly client (a few minutes the first time)');
  run(['rustup', 'target', 'add', 'wasm32-unknown-unknown'], root);
  run(['wasm-pack', 'build', '--target', 'nodejs', '--out-dir', 'wasm'], clientNode, wasmToolchainEnv());
}

step('Installing JS dependencies');
// Drop links from an earlier setup first, so the installer never deletes through them
for (const path of [join(framework, 'node_modules', 'firefly-client-node')]) {
  if (isLink(path)) unlinkSync(path);
}
run(['bun', 'install', '--no-save'], clientNode);
run(['bun', 'install', '--no-save'], framework);

step('Linking packages');
link(join(framework, 'node_modules', 'firefly-client-node'), clientNode);
console.log('crates/firefly-client-js -> crates/client-node');
for (const dir of frameworkUsers) {
  const exampleDir = join(root, 'examples', dir);
  if (!existsSync(exampleDir)) continue;
  link(join(exampleDir, 'node_modules', 'firefly-client-js'), framework);
  console.log(`examples/${dir} -> crates/firefly-client-js`);
}

step('Compiling TypeScript');
run(['node', join('node_modules', 'typescript', 'bin', 'tsc')], clientNode);
run(['node', join('node_modules', 'typescript', 'bin', 'tsc'), '-p', '.'], framework);

step('Preparing bot config');
const needsCredentials: string[] = [];
for (const bot of bots) {
  const dir = join(root, 'examples', bot);
  const env = join(dir, '.env');
  if (!existsSync(env)) {
    copyFileSync(join(dir, '.env.example'), env);
    console.log(`created examples/${bot}/.env`);
  }
  const content = await Bun.file(env).text();
  if (!/^BOT_EMAIL=\S+/m.test(content) || !/^BOT_PASSWORD=\S+/m.test(content)) needsCredentials.push(bot);
}

console.log('\nSetup complete.');
if (needsCredentials.length > 0) {
  console.log('\nNext: put each bot\'s Lupyd account (email + password) into:');
  for (const bot of needsCredentials) console.log(`  examples/${bot}/.env`);
}
console.log('\nThen start the bots:  bun examples/run-bots.ts');
