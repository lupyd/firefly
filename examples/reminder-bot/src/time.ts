// Time zone helpers built on Intl, so the bot needs no date library.

export interface LocalDate {
  year: number;
  // 0-based, like Date
  month: number;
  day: number;
}

export interface LocalDateTime extends LocalDate {
  hour: number;
  minute: number;
  // 0 = Sunday
  weekday: number;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function partsFormatter(timeZone: string): Intl.DateTimeFormat {
  let formatter = formatters.get(timeZone);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat('en-US', {
      timeZone,
      hourCycle: 'h23',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    formatters.set(timeZone, formatter);
  }
  return formatter;
}

export function isValidTimeZone(timeZone: string): boolean {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone });
    return true;
  } catch {
    return false;
  }
}

export function toLocal(utcMs: number, timeZone: string): LocalDateTime {
  const parts = partsFormatter(timeZone).formatToParts(new Date(utcMs));
  const get = (type: Intl.DateTimeFormatPartTypes) => Number(parts.find((p) => p.type === type)?.value);
  const year = get('year');
  const month = get('month') - 1;
  const day = get('day');
  return {
    year,
    month,
    day,
    hour: get('hour'),
    minute: get('minute'),
    weekday: new Date(Date.UTC(year, month, day)).getUTCDay(),
  };
}

function offsetMs(utcMs: number, timeZone: string): number {
  const local = toLocal(utcMs, timeZone);
  const seconds = new Date(utcMs).getUTCSeconds();
  const asUtc = Date.UTC(local.year, local.month, local.day, local.hour, local.minute, seconds);
  return asUtc - (utcMs - (utcMs % 1000));
}

// Wall-clock time in a zone -> UTC instant. Times skipped by a DST jump resolve to just after it.
export function fromLocal(date: LocalDate, hour: number, minute: number, timeZone: string): number {
  const guess = Date.UTC(date.year, date.month, date.day, hour, minute);
  const first = guess - offsetMs(guess, timeZone);
  return guess - offsetMs(first, timeZone);
}

export function addDays(date: LocalDate, days: number): LocalDate {
  const d = new Date(Date.UTC(date.year, date.month, date.day + days));
  return { year: d.getUTCFullYear(), month: d.getUTCMonth(), day: d.getUTCDate() };
}

export function formatInZone(utcMs: number, timeZone: string): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone,
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(new Date(utcMs));
}

export function formatClock(hour: number, minute: number): string {
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}
