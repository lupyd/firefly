import { Database } from 'bun:sqlite';
import type { RepeatRule } from './parse';

export const DEFAULT_TIME_ZONE = 'UTC';

export interface NewReminder {
  creator: string;
  // Where it is delivered: a group channel, or a direct message to dmTo
  groupId: number | null;
  channelId: number | null;
  dmTo: string | null;
  // Username mentioned in the delivered message
  mention: string;
  text: string;
  dueAt: number;
  repeat: RepeatRule | null;
  timeZone: string;
}

export interface Reminder extends NewReminder {
  id: number;
  attempts: number;
}

interface ReminderRow {
  id: number;
  creator: string;
  group_id: number | null;
  channel_id: number | null;
  dm_to: string | null;
  mention: string;
  text: string;
  due_at: number;
  repeat: string | null;
  time_zone: string;
  attempts: number;
}

function fromRow(row: ReminderRow): Reminder {
  return {
    id: row.id,
    creator: row.creator,
    groupId: row.group_id,
    channelId: row.channel_id,
    dmTo: row.dm_to,
    mention: row.mention,
    text: row.text,
    dueAt: row.due_at,
    repeat: row.repeat ? (JSON.parse(row.repeat) as RepeatRule) : null,
    timeZone: row.time_zone,
    attempts: row.attempts,
  };
}

export class ReminderStore {
  private db: Database;

  constructor(path: string) {
    this.db = new Database(path, { create: true });
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run(`
      CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator TEXT NOT NULL,
        group_id INTEGER,
        channel_id INTEGER,
        dm_to TEXT,
        mention TEXT NOT NULL,
        text TEXT NOT NULL,
        due_at INTEGER NOT NULL,
        repeat TEXT,
        time_zone TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
      )
    `);
    this.db.run('CREATE INDEX IF NOT EXISTS reminders_due_at ON reminders (due_at)');
    this.db.run('CREATE INDEX IF NOT EXISTS reminders_creator ON reminders (creator)');
    this.db.run('CREATE TABLE IF NOT EXISTS user_settings (username TEXT PRIMARY KEY, time_zone TEXT NOT NULL)');
  }

  add(reminder: NewReminder): number {
    const result = this.db.run(
      `INSERT INTO reminders (creator, group_id, channel_id, dm_to, mention, text, due_at, repeat, time_zone)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        reminder.creator,
        reminder.groupId,
        reminder.channelId,
        reminder.dmTo,
        reminder.mention,
        reminder.text,
        reminder.dueAt,
        reminder.repeat ? JSON.stringify(reminder.repeat) : null,
        reminder.timeZone,
      ],
    );
    return Number(result.lastInsertRowid);
  }

  countByCreator(creator: string): number {
    return this.db.query<{ n: number }, [string]>('SELECT COUNT(*) AS n FROM reminders WHERE creator = ?').get(creator)?.n ?? 0;
  }

  listByCreator(creator: string): Reminder[] {
    return this.db
      .query<ReminderRow, [string]>('SELECT * FROM reminders WHERE creator = ? ORDER BY due_at')
      .all(creator)
      .map(fromRow);
  }

  // Only the creator can delete a reminder
  delete(id: number, creator: string): boolean {
    return this.db.run('DELETE FROM reminders WHERE id = ? AND creator = ?', [id, creator]).changes > 0;
  }

  remove(id: number): void {
    this.db.run('DELETE FROM reminders WHERE id = ?', [id]);
  }

  due(now: number, limit = 50): Reminder[] {
    return this.db
      .query<ReminderRow, [number, number]>('SELECT * FROM reminders WHERE due_at <= ? ORDER BY due_at LIMIT ?')
      .all(now, limit)
      .map(fromRow);
  }

  reschedule(id: number, dueAt: number): void {
    this.db.run('UPDATE reminders SET due_at = ?, attempts = 0 WHERE id = ?', [dueAt, id]);
  }

  // Returns the new attempt count
  recordFailure(id: number, retryAt: number): number {
    this.db.run('UPDATE reminders SET due_at = ?, attempts = attempts + 1 WHERE id = ?', [retryAt, id]);
    return this.db.query<{ attempts: number }, [number]>('SELECT attempts FROM reminders WHERE id = ?').get(id)?.attempts ?? 0;
  }

  getTimeZone(username: string): string {
    return this.db
      .query<{ time_zone: string }, [string]>('SELECT time_zone FROM user_settings WHERE username = ?')
      .get(username)?.time_zone ?? DEFAULT_TIME_ZONE;
  }

  hasTimeZone(username: string): boolean {
    return this.db.query('SELECT 1 FROM user_settings WHERE username = ?').get(username) !== null;
  }

  setTimeZone(username: string, timeZone: string): void {
    this.db.run('INSERT OR REPLACE INTO user_settings (username, time_zone) VALUES (?, ?)', [username, timeZone]);
  }

  close(): void {
    this.db.close();
  }
}
