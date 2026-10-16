import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { SqliteBotStorage } from './sqlite-storage';

const bytes = (...values: number[]) => new Uint8Array(values);
const dirs: string[] = [];

function tempDbPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'bot-storage-'));
  dirs.push(dir);
  return join(dir, 'state.db');
}

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('SqliteBotStorage', () => {
  test('keeps everything across a restart', async () => {
    const path = tempDbPath();
    const first = new SqliteBotStorage(path);
    const p = first.providers;
    await p.signalStateStorage!.save(bytes(1, 2, 3));
    await p.keyValueStorage!.set('group_identity_b64', 'abc');
    await p.mlsKeyPackageStorage!.insert(bytes(9), bytes(4, 5));
    await p.mlsGroupStateStorage!.write(bytes(7), bytes(8), { '1': bytes(10) }, new Map([[2, bytes(11)]]));
    await p.groupInfoStorage!.set(42, 'Design', 'Team', bytes(7));
    first.close();

    const second = new SqliteBotStorage(path);
    const q = second.providers;
    expect(await q.signalStateStorage!.load()).toEqual(bytes(1, 2, 3));
    expect(await q.keyValueStorage!.get('group_identity_b64')).toBe('abc');
    expect(await q.mlsKeyPackageStorage!.get(bytes(9))).toEqual(bytes(4, 5));
    expect(await q.mlsGroupStateStorage!.state(bytes(7))).toEqual(bytes(8));
    expect(await q.mlsGroupStateStorage!.epoch(bytes(7), 1)).toEqual(bytes(10));
    expect(await q.mlsGroupStateStorage!.maxEpochId!(bytes(7))).toBe(2);
    expect(await q.groupInfoStorage!.getAll!()).toEqual([{ id: 42, name: 'Design', description: 'Team', identifier: bytes(7) }]);
    second.close();
  });

  test('reports missing values as null so the client falls back to its defaults', async () => {
    const storage = new SqliteBotStorage(':memory:');
    const p = storage.providers;
    expect(await p.signalStateStorage!.load()).toBeNull();
    expect(await p.keyValueStorage!.get('missing')).toBeNull();
    expect(await p.mlsKeyPackageStorage!.get(bytes(1))).toBeNull();
    expect(await p.mlsGroupStateStorage!.state(bytes(1))).toBeNull();
    expect(await p.mlsGroupStateStorage!.maxEpochId!(bytes(1))).toBeNull();
    expect(await p.groupInfoStorage!.get(1)).toBeNull();
    storage.close();
  });

  test('overwrites and deletes', async () => {
    const storage = new SqliteBotStorage(':memory:');
    const p = storage.providers;
    await p.signalStateStorage!.save(bytes(1));
    await p.signalStateStorage!.save(bytes(2));
    expect(await p.signalStateStorage!.load()).toEqual(bytes(2));

    expect(await p.mlsKeyPackageStorage!.delete(bytes(9))).toBe(false);
    await p.mlsKeyPackageStorage!.insert(bytes(9), bytes(1));
    expect(await p.mlsKeyPackageStorage!.delete(bytes(9))).toBe(true);
    expect(await p.mlsKeyPackageStorage!.get(bytes(9))).toBeNull();

    await p.groupInfoStorage!.set(1, 'A', '', bytes(1));
    await p.groupInfoStorage!.delete(1);
    expect(await p.groupInfoStorage!.getAll!()).toEqual([]);
    storage.close();
  });
});
