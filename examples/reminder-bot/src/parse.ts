import { addDays, formatClock, fromLocal, toLocal, type LocalDate } from './time';

export type Target = { kind: 'me' } | { kind: 'here' } | { kind: 'user'; username: string };

export type RepeatRule =
  | { kind: 'daily'; hour: number; minute: number }
  | { kind: 'weekdays'; hour: number; minute: number }
  | { kind: 'weekly'; weekday: number; hour: number; minute: number };

export interface ParsedReminder {
  target: Target;
  dueAt: number;
  repeat: RepeatRule | null;
  text: string;
  // "in 2h" style, so the time zone does not matter
  relative: boolean;
}

export type ParseResult = { ok: true; reminder: ParsedReminder } | { ok: false; error: string };

export const MIN_DELAY_MS = 60_000;
export const MAX_DELAY_MS = 365 * 24 * 60 * 60_000;
export const MAX_TEXT_LENGTH = 500;

const DEFAULT_HOUR = 9;

const WEEKDAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
const WEEKDAY_NAMES = WEEKDAYS.map((d) => d[0].toUpperCase() + d.slice(1));

const UNIT_MS: Record<string, number> = {
  m: 60_000, min: 60_000, mins: 60_000, minute: 60_000, minutes: 60_000,
  h: 3_600_000, hr: 3_600_000, hrs: 3_600_000, hour: 3_600_000, hours: 3_600_000,
  d: 86_400_000, day: 86_400_000, days: 86_400_000,
  w: 604_800_000, week: 604_800_000, weeks: 604_800_000,
};

function parseWeekday(token: string | undefined): number | null {
  if (!token) return null;
  const index = WEEKDAYS.findIndex((day) => day === token || (token.length >= 3 && day.startsWith(token)));
  return index === -1 ? null : index;
}

interface ClockTime {
  hour: number;
  minute: number;
  consumed: number;
}

// Parses "9am", "9:30 pm", "21:30", "noon", "midnight". A bare number like "9" counts as an
// hour only when allowBareHour is set (after an explicit "at"), so "tomorrow 5 apples" stays text.
function parseClock(tokens: string[], i: number, allowBareHour: boolean): ClockTime | null {
  const token = tokens[i];
  if (!token) return null;
  if (token === 'noon') return { hour: 12, minute: 0, consumed: 1 };
  if (token === 'midnight') return { hour: 0, minute: 0, consumed: 1 };

  const match = /^(\d{1,2})(?::(\d{2}))?(am|pm)?$/.exec(token);
  if (!match) return null;
  let consumed = 1;
  let meridiem = match[3];
  if (!meridiem && (tokens[i + 1] === 'am' || tokens[i + 1] === 'pm')) {
    meridiem = tokens[i + 1];
    consumed = 2;
  }
  if (!meridiem && match[2] === undefined && !allowBareHour) return null;

  let hour = Number(match[1]);
  const minute = match[2] === undefined ? 0 : Number(match[2]);
  if (minute > 59) return null;
  if (meridiem) {
    if (hour < 1 || hour > 12) return null;
    hour = (hour % 12) + (meridiem === 'pm' ? 12 : 0);
  } else if (hour > 23) {
    return null;
  }
  return { hour, minute, consumed };
}

// "[at] <time>" with a fallback of 09:00 when no time is given
function optionalClock(tokens: string[], i: number): ClockTime | { error: string } {
  if (tokens[i] === 'at') {
    const clock = parseClock(tokens, i + 1, true);
    if (!clock) return { error: 'I could not read the time after "at". Try 9am, 14:30 or noon.' };
    return { ...clock, consumed: clock.consumed + 1 };
  }
  return parseClock(tokens, i, false) ?? { hour: DEFAULT_HOUR, minute: 0, consumed: 0 };
}

function parseDuration(tokens: string[], i: number): { ms: number; consumed: number } | null {
  let ms = 0;
  let j = i;
  while (j < tokens.length) {
    const token = tokens[j];
    const compound = /^(\d+[a-z]+)+$/.test(token) ? [...token.matchAll(/(\d+)([a-z]+)/g)] : null;
    if (compound && compound.every((m) => UNIT_MS[m[2]] !== undefined)) {
      for (const m of compound) ms += Number(m[1]) * UNIT_MS[m[2]];
      j += 1;
    } else if (/^\d+$/.test(token) && UNIT_MS[tokens[j + 1]] !== undefined) {
      ms += Number(token) * UNIT_MS[tokens[j + 1]];
      j += 2;
    } else if (token === 'and' && j > i) {
      j += 1;
    } else {
      break;
    }
  }
  return j === i ? null : { ms, consumed: j - i };
}

export function nextOccurrence(rule: RepeatRule, after: number, timeZone: string): number {
  const today = toLocal(after, timeZone);
  for (let i = 0; i <= 8; i++) {
    const date = addDays(today, i);
    const weekday = new Date(Date.UTC(date.year, date.month, date.day)).getUTCDay();
    const matches =
      rule.kind === 'daily' ||
      (rule.kind === 'weekdays' && weekday >= 1 && weekday <= 5) ||
      (rule.kind === 'weekly' && weekday === rule.weekday);
    if (!matches) continue;
    const at = fromLocal(date, rule.hour, rule.minute, timeZone);
    if (at > after) return at;
  }
  throw new Error('no upcoming occurrence');
}

export function describeRepeat(rule: RepeatRule): string {
  const clock = formatClock(rule.hour, rule.minute);
  if (rule.kind === 'daily') return `every day at ${clock}`;
  if (rule.kind === 'weekdays') return `every weekday at ${clock}`;
  return `every ${WEEKDAY_NAMES[rule.weekday]} at ${clock}`;
}

type When = { dueAt: number; repeat: RepeatRule | null; consumed: number } | { error: string };

function parseWhen(tokens: string[], now: number, timeZone: string): When {
  const today: LocalDate = toLocal(now, timeZone);
  const head = tokens[0];

  if (head === 'in') {
    const duration = parseDuration(tokens, 1);
    if (!duration) return { error: 'Tell me how long, for example: in 10m, in 2h, in 1h30m or in 3 days.' };
    return { dueAt: now + duration.ms, repeat: null, consumed: 1 + duration.consumed };
  }

  if (head === 'at') {
    const clock = parseClock(tokens, 1, true);
    if (!clock) return { error: 'I could not read the time after "at". Try 9am, 14:30 or noon.' };
    let dueAt = fromLocal(today, clock.hour, clock.minute, timeZone);
    if (dueAt <= now) dueAt = fromLocal(addDays(today, 1), clock.hour, clock.minute, timeZone);
    return { dueAt, repeat: null, consumed: 1 + clock.consumed };
  }

  if (head === 'today' || head === 'tomorrow') {
    const clock = optionalClock(tokens, 1);
    if ('error' in clock) return clock;
    const date = head === 'today' ? today : addDays(today, 1);
    return { dueAt: fromLocal(date, clock.hour, clock.minute, timeZone), repeat: null, consumed: 1 + clock.consumed };
  }

  if (head === 'every') {
    const what = tokens[1];
    const weekday = parseWeekday(what);
    const kind = what === 'day' ? 'daily' : what === 'weekday' || what === 'weekdays' ? 'weekdays' : weekday !== null ? 'weekly' : null;
    if (!kind) return { error: 'Repeat with every day, every weekday or every monday (any day of the week).' };
    const clock = optionalClock(tokens, 2);
    if ('error' in clock) return clock;
    const repeat: RepeatRule = kind === 'weekly'
      ? { kind, weekday: weekday!, hour: clock.hour, minute: clock.minute }
      : { kind, hour: clock.hour, minute: clock.minute };
    return { dueAt: nextOccurrence(repeat, now, timeZone), repeat, consumed: 2 + clock.consumed };
  }

  // "on 2026-10-20", "on friday", or just "friday"
  const offset = head === 'on' ? 1 : 0;
  const dayToken = tokens[offset];
  const isoDate = dayToken ? /^(\d{4})-(\d{2})-(\d{2})$/.exec(dayToken) : null;
  const weekday = parseWeekday(dayToken);
  if (isoDate || weekday !== null) {
    const clock = optionalClock(tokens, offset + 1);
    if ('error' in clock) return clock;
    const consumed = offset + 1 + clock.consumed;
    if (isoDate) {
      const date = { year: Number(isoDate[1]), month: Number(isoDate[2]) - 1, day: Number(isoDate[3]) };
      return { dueAt: fromLocal(date, clock.hour, clock.minute, timeZone), repeat: null, consumed };
    }
    const once: RepeatRule = { kind: 'weekly', weekday: weekday!, hour: clock.hour, minute: clock.minute };
    return { dueAt: nextOccurrence(once, now, timeZone), repeat: null, consumed };
  }

  return { error: 'Tell me when, for example: in 2h, at 5pm, tomorrow at 9am, on friday, or every monday at 10am.' };
}

function parseTarget(token: string | undefined): { target: Target; consumed: number } | { error: string } {
  if (token === 'me') return { target: { kind: 'me' }, consumed: 1 };
  if (token === 'here') return { target: { kind: 'here' }, consumed: 1 };
  if (token?.startsWith('@')) {
    const username = token.slice(1);
    if (!/^[\w.-]{1,64}$/.test(username)) return { error: `"${token}" is not a valid username.` };
    return { target: { kind: 'user', username }, consumed: 1 };
  }
  return { target: { kind: 'me' }, consumed: 0 };
}

// Parses the arguments of /remind, e.g. ["me", "in", "2h", "check", "the", "deploy"]
export function parseRemind(args: string[], now: number, timeZone: string): ParseResult {
  const lower = args.map((a) => a.toLowerCase());
  const target = parseTarget(args[0]);
  if ('error' in target) return { ok: false, error: target.error };

  const when = parseWhen(lower.slice(target.consumed), now, timeZone);
  if ('error' in when) return { ok: false, error: when.error };

  const rest = args.slice(target.consumed + when.consumed);
  if (rest[0]?.toLowerCase() === 'to') rest.shift();
  const text = rest.join(' ').trim();

  if (!text) return { ok: false, error: 'What should I remind you about? Add it after the time.' };
  if (text.length > MAX_TEXT_LENGTH) return { ok: false, error: `Keep reminders under ${MAX_TEXT_LENGTH} characters.` };
  if (when.dueAt <= now) return { ok: false, error: 'That time has already passed.' };
  if (!when.repeat && when.dueAt - now < MIN_DELAY_MS) return { ok: false, error: 'Reminders need to be at least 1 minute away.' };
  if (when.dueAt - now > MAX_DELAY_MS) return { ok: false, error: 'Reminders can be at most a year away.' };

  const relative = lower[target.consumed] === 'in';
  return { ok: true, reminder: { target: target.target, dueAt: when.dueAt, repeat: when.repeat, text, relative } };
}
