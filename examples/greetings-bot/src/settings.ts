import { Database } from 'bun:sqlite';
import { DEFAULT_DM, DEFAULT_WELCOME } from './template';

export interface GroupSettings {
  groupId: number;
  enabled: boolean;
  // null = the group's first channel
  channelId: number | null;
  template: string;
  dmEnabled: boolean;
  dmTemplate: string;
}

interface SettingsRow {
  group_id: number;
  enabled: number;
  channel_id: number | null;
  template: string;
  dm_enabled: number;
  dm_template: string;
}

export class SettingsStore {
  private db: Database;

  constructor(path: string) {
    this.db = new Database(path, { create: true });
    this.db.run(`
      CREATE TABLE IF NOT EXISTS group_settings (
        group_id INTEGER PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 1,
        channel_id INTEGER,
        template TEXT NOT NULL,
        dm_enabled INTEGER NOT NULL DEFAULT 0,
        dm_template TEXT NOT NULL
      )
    `);
  }

  get(groupId: number): GroupSettings {
    const row = this.db
      .query<SettingsRow, [number]>('SELECT * FROM group_settings WHERE group_id = ?')
      .get(groupId);
    if (!row) {
      return { groupId, enabled: true, channelId: null, template: DEFAULT_WELCOME, dmEnabled: false, dmTemplate: DEFAULT_DM };
    }
    return {
      groupId,
      enabled: row.enabled === 1,
      channelId: row.channel_id,
      template: row.template,
      dmEnabled: row.dm_enabled === 1,
      dmTemplate: row.dm_template,
    };
  }

  update(groupId: number, changes: Partial<Omit<GroupSettings, 'groupId'>>): GroupSettings {
    const next = { ...this.get(groupId), ...changes };
    this.db.run(
      `INSERT OR REPLACE INTO group_settings (group_id, enabled, channel_id, template, dm_enabled, dm_template)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [groupId, next.enabled ? 1 : 0, next.channelId, next.template, next.dmEnabled ? 1 : 0, next.dmTemplate],
    );
    return next;
  }

  close(): void {
    this.db.close();
  }
}
