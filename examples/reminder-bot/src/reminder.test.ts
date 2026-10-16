import { describe, expect, test } from 'bun:test';
import { parseRemind, type ParsedReminder } from './parse';
import { deliverDue, formatReminder, MAX_ATTEMPTS } from './scheduler';
import { ReminderStore, type NewReminder } from './store';
import { fromLocal, isValidTimeZone } from './time';

// Friday 16 Oct 2026, 08:00 UTC = 13:30 in Asia/Kolkata (UTC+5:30)
const NOW = Date.UTC(2026, 9, 16, 8, 0);
const IST = 'Asia/Kolkata';
const MIN = 60_000;

function parse(command: string, timeZone = IST): ParsedReminder {
  const result = parseRemind(command.split(/\s+/), NOW, timeZone);
  if (!result.ok) throw new Error(`expected "${command}" to parse: ${result.error}`);
  return result.reminder;
}

function parseError(command: string): string {
  const result = parseRemind(command.split(/\s+/), NOW, IST);
  if (result.ok) throw new Error(`expected "${command}" to fail`);
  return result.error;
}

describe('time zones', () => {
  test('converts wall-clock time to UTC across a DST change', () => {
    expect(fromLocal({ year: 2026, month: 0, day: 15 }, 12, 0, 'America/New_York')).toBe(Date.UTC(2026, 0, 15, 17, 0));
    expect(fromLocal({ year: 2026, month: 2, day: 9 }, 12, 0, 'America/New_York')).toBe(Date.UTC(2026, 2, 9, 16, 0));
    expect(fromLocal({ year: 2026, month: 9, day: 16 }, 17, 0, IST)).toBe(Date.UTC(2026, 9, 16, 11, 30));
  });

  test('validates zone names', () => {
    expect(isValidTimeZone('Asia/Kolkata')).toBe(true);
    expect(isValidTimeZone('Mars/Olympus')).toBe(false);
  });
});

describe('parseRemind', () => {
  test('relative durations', () => {
    expect(parse('me in 2h check the deploy')).toMatchObject({ target: { kind: 'me' }, dueAt: NOW + 120 * MIN, text: 'check the deploy', relative: true });
    expect(parse('in 1h30m to stretch')).toMatchObject({ dueAt: NOW + 90 * MIN, text: 'stretch' });
    expect(parse('me in 2 hours and 15 minutes tea').dueAt).toBe(NOW + 135 * MIN);
    expect(parse('in 3 days renew').dueAt).toBe(NOW + 3 * 24 * 60 * MIN);
  });

  test('clock times roll to tomorrow once passed', () => {
    expect(parse('at 5pm call mom')).toMatchObject({ dueAt: Date.UTC(2026, 9, 16, 11, 30), relative: false });
    expect(parse('at 9am standup').dueAt).toBe(Date.UTC(2026, 9, 17, 3, 30));
    expect(parse('at 21:45 lights').dueAt).toBe(Date.UTC(2026, 9, 16, 16, 15));
  });

  test('tomorrow defaults to 9am and does not eat numbers from the text', () => {
    expect(parse('tomorrow send report').dueAt).toBe(Date.UTC(2026, 9, 17, 3, 30));
    expect(parse('tomorrow 5 apples')).toMatchObject({ dueAt: Date.UTC(2026, 9, 17, 3, 30), text: '5 apples' });
    expect(parse('tomorrow at noon lunch').dueAt).toBe(Date.UTC(2026, 9, 17, 6, 30));
  });

  test('weekdays and dates', () => {
    expect(parse('here on friday at 6pm demo')).toMatchObject({ target: { kind: 'here' }, dueAt: Date.UTC(2026, 9, 16, 12, 30) });
    expect(parse('monday 10am sync').dueAt).toBe(Date.UTC(2026, 9, 19, 4, 30));
    expect(parse('on 2026-12-24 at 8pm gifts').dueAt).toBe(Date.UTC(2026, 11, 24, 14, 30));
  });

  test('repeating reminders start at the next matching day', () => {
    expect(parse('here every monday at 10am weekly sync')).toMatchObject({
      dueAt: Date.UTC(2026, 9, 19, 4, 30),
      repeat: { kind: 'weekly', weekday: 1, hour: 10, minute: 0 },
      text: 'weekly sync',
    });
    expect(parse('every weekday at 9am standup').dueAt).toBe(Date.UTC(2026, 9, 19, 3, 30));
    expect(parse('every day at 6pm water plants').dueAt).toBe(Date.UTC(2026, 9, 16, 12, 30));
  });

  test('mentions another user', () => {
    expect(parse('@bob in 30m review my PR')).toMatchObject({ target: { kind: 'user', username: 'bob' }, text: 'review my PR' });
  });

  test('explains what is wrong', () => {
    expect(parseError('me check the oven')).toContain('when');
    expect(parseError('me in 5m')).toContain('What should I remind you about');
    expect(parseError('in 0m now')).toContain('already passed');
    expect(parseError('at 25:00 late')).toContain('time');
    expect(parseError('on 2026-10-01 too late')).toContain('already passed');
    expect(parseError('in 400 days far')).toContain('year');
    expect(parseError('@bad!name in 5m hi')).toContain('not a valid username');
  });
});

function newReminder(changes: Partial<NewReminder> = {}): NewReminder {
  return {
    creator: 'alice',
    groupId: null,
    channelId: null,
    dmTo: 'alice',
    mention: 'alice',
    text: 'drink water',
    dueAt: NOW - MIN,
    repeat: null,
    timeZone: IST,
    ...changes,
  };
}

describe('deliverDue', () => {
  test('sends due one-off reminders once and removes them', async () => {
    const store = new ReminderStore(':memory:');
    store.add(newReminder());
    store.add(newReminder({ dueAt: NOW + 10 * MIN, text: 'later' }));
    const sent: string[] = [];
    expect(await deliverDue(store, NOW, async (_r, message) => { sent.push(message); })).toBe(1);
    expect(sent).toEqual(['⏰ Reminder for @alice: drink water']);
    expect(store.listByCreator('alice').map((r) => r.text)).toEqual(['later']);
    store.close();
  });

  test('moves repeating reminders to the next occurrence after now', async () => {
    const store = new ReminderStore(':memory:');
    const id = store.add(newReminder({ dueAt: NOW - 3 * 24 * 60 * MIN, repeat: { kind: 'daily', hour: 9, minute: 0 } }));
    const sent: string[] = [];
    await deliverDue(store, NOW, async (_r, message) => { sent.push(message); });
    expect(sent).toHaveLength(1);
    expect(sent[0]).toContain('delivered late');
    expect(store.listByCreator('alice').find((r) => r.id === id)?.dueAt).toBe(Date.UTC(2026, 9, 17, 3, 30));
    store.close();
  });

  test('retries failed deliveries and gives up after the limit', async () => {
    const store = new ReminderStore(':memory:');
    store.add(newReminder());
    let now = NOW;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      await deliverDue(store, now, async () => { throw new Error('offline'); });
      now += 10 * MIN;
    }
    expect(store.countByCreator('alice')).toBe(0);
    store.close();
  });

  test('names who set a reminder for someone else', () => {
    const message = formatReminder({ ...newReminder({ mention: 'bob', dueAt: NOW }), id: 1, attempts: 0 }, NOW);
    expect(message).toBe('⏰ Reminder for @bob: drink water\n(set by @alice)');
  });
});

describe('ReminderStore', () => {
  test('only the creator can cancel', () => {
    const store = new ReminderStore(':memory:');
    const id = store.add(newReminder());
    expect(store.delete(id, 'mallory')).toBe(false);
    expect(store.delete(id, 'alice')).toBe(true);
    store.close();
  });

  test('time zone defaults to UTC', () => {
    const store = new ReminderStore(':memory:');
    expect(store.getTimeZone('alice')).toBe('UTC');
    store.setTimeZone('alice', IST);
    expect(store.getTimeZone('alice')).toBe(IST);
    expect(store.hasTimeZone('alice')).toBe(true);
    store.close();
  });
});
