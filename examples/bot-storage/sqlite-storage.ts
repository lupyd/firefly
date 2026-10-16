import { Database, type Statement } from 'bun:sqlite';
import { chmodSync } from 'fs';
import type { StorageProviders } from 'firefly-client-js';

type EpochEntries = Record<string, Uint8Array> | Map<number | bigint, Uint8Array>;

function epochEntries(entries: EpochEntries): Array<[number, Uint8Array]> {
  const list = entries instanceof Map ? [...entries.entries()] : Object.entries(entries);
  return list.map(([epoch, data]) => [Number(epoch), data]);
}

interface GroupInfoRow {
  id: number;
  name: string;
  description: string;
  identifier: Uint8Array;
}

/**
 * Persists a bot's encryption state in one SQLite file so it keeps its device, keys and
 * group memberships across restarts: the Signal snapshot, the MLS identity (in the
 * key-value table), MLS key packages and group state, and the list of groups.
 * Message contents are not stored. The file holds private keys, so it is created owner-only
 * where the platform supports it.
 */
export class SqliteBotStorage {
  readonly providers: StorageProviders;
  private db: Database;
  private writeEpoch: Statement;

  constructor(path: string) {
    this.db = new Database(path, { create: true });
    try {
      chmodSync(path, 0o600);
    } catch {
      // Windows and in-memory databases have no POSIX permissions
    }
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run('CREATE TABLE IF NOT EXISTS key_value (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
    this.db.run('CREATE TABLE IF NOT EXISTS signal_state (id INTEGER PRIMARY KEY CHECK (id = 1), snapshot BLOB NOT NULL)');
    this.db.run('CREATE TABLE IF NOT EXISTS mls_key_packages (id BLOB PRIMARY KEY, data BLOB NOT NULL)');
    this.db.run('CREATE TABLE IF NOT EXISTS mls_group_state (group_id BLOB PRIMARY KEY, state BLOB NOT NULL)');
    this.db.run(`CREATE TABLE IF NOT EXISTS mls_epochs (
      group_id BLOB NOT NULL, epoch_id INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (group_id, epoch_id))`);
    this.db.run(`CREATE TABLE IF NOT EXISTS group_info (
      id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, identifier BLOB NOT NULL)`);

    const db = this.db;
    const writeEpoch = (this.writeEpoch = db.prepare('INSERT OR REPLACE INTO mls_epochs (group_id, epoch_id, data) VALUES (?, ?, ?)'));
    const writeGroupState = db.transaction((groupId: Uint8Array, state: Uint8Array, inserts: EpochEntries, updates: EpochEntries) => {
      db.run('INSERT OR REPLACE INTO mls_group_state (group_id, state) VALUES (?, ?)', [groupId, state]);
      for (const [epoch, data] of [...epochEntries(inserts), ...epochEntries(updates)]) {
        writeEpoch.run(groupId, epoch, data);
      }
    });

    this.providers = {
      signalStateStorage: {
        load: () => db.query<{ snapshot: Uint8Array }, []>('SELECT snapshot FROM signal_state WHERE id = 1').get()?.snapshot ?? null,
        save: (snapshot) => {
          db.run('INSERT OR REPLACE INTO signal_state (id, snapshot) VALUES (1, ?)', [snapshot]);
        },
      },
      keyValueStorage: {
        get: async (key) => db.query<{ value: string }, [string]>('SELECT value FROM key_value WHERE key = ?').get(key)?.value ?? null,
        set: (key, value) => {
          db.run('INSERT OR REPLACE INTO key_value (key, value) VALUES (?, ?)', [key, value]);
        },
        updateLastReceivedMessageId: (id) => {
          db.run('INSERT OR REPLACE INTO key_value (key, value) VALUES (?, ?)', ['last_received_message_id', String(id)]);
        },
      },
      mlsKeyPackageStorage: {
        insert: (id, data) => db.run('INSERT OR REPLACE INTO mls_key_packages (id, data) VALUES (?, ?)', [id, data]).changes > 0,
        delete: (id) => db.run('DELETE FROM mls_key_packages WHERE id = ?', [id]).changes > 0,
        get: (id) => db.query<{ data: Uint8Array }, [Uint8Array]>('SELECT data FROM mls_key_packages WHERE id = ?').get(id)?.data ?? null,
      },
      mlsGroupStateStorage: {
        state: (groupId) => db.query<{ state: Uint8Array }, [Uint8Array]>('SELECT state FROM mls_group_state WHERE group_id = ?').get(groupId)?.state ?? null,
        epoch: (groupId, epochId) =>
          db.query<{ data: Uint8Array }, [Uint8Array, number]>('SELECT data FROM mls_epochs WHERE group_id = ? AND epoch_id = ?')
            .get(groupId, Number(epochId))?.data ?? null,
        write: (groupId, state, inserts, updates) => {
          writeGroupState(groupId, state, inserts, updates);
          return true;
        },
        maxEpochId: (groupId) =>
          db.query<{ max: number | null }, [Uint8Array]>('SELECT MAX(epoch_id) AS max FROM mls_epochs WHERE group_id = ?').get(groupId)?.max ?? null,
      },
      groupInfoStorage: {
        getAll: async () => db.query<GroupInfoRow, []>('SELECT * FROM group_info').all(),
        get: async (id) => db.query<GroupInfoRow, [number]>('SELECT * FROM group_info WHERE id = ?').get(Number(id)),
        set: (id, name, description, identifier) => {
          db.run('INSERT OR REPLACE INTO group_info (id, name, description, identifier) VALUES (?, ?, ?, ?)', [Number(id), name, description, identifier]);
        },
        delete: (id) => {
          db.run('DELETE FROM group_info WHERE id = ?', [Number(id)]);
        },
      },
    };
  }

  close(): void {
    // An unfinalized prepared statement keeps the file open (and locked on Windows)
    this.writeEpoch.finalize();
    this.db.close();
  }
}
