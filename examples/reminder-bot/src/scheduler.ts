import { describeRepeat, nextOccurrence } from './parse';
import type { Reminder, ReminderStore } from './store';

// Delivered this long after the due time, a reminder is marked as late
export const LATE_AFTER_MS = 2 * 60_000;
export const RETRY_DELAY_MS = 5 * 60_000;
export const MAX_ATTEMPTS = 5;

export type SendReminder = (reminder: Reminder, message: string) => Promise<void>;

export function formatReminder(reminder: Reminder, now: number): string {
  const lines = [`⏰ Reminder for @${reminder.mention}: ${reminder.text}`];
  if (reminder.mention !== reminder.creator) lines.push(`(set by @${reminder.creator})`);
  if (reminder.repeat) lines.push(`(repeats ${describeRepeat(reminder.repeat)})`);
  if (now - reminder.dueAt > LATE_AFTER_MS) lines.push('(delivered late: I was offline at the scheduled time)');
  return lines.join('\n');
}

// Sends every due reminder once. Repeating reminders move to their next occurrence after now,
// so a bot that was offline for days sends one catch-up message rather than one per missed day.
export async function deliverDue(store: ReminderStore, now: number, send: SendReminder): Promise<number> {
  let delivered = 0;
  for (const reminder of store.due(now)) {
    try {
      await send(reminder, formatReminder(reminder, now));
      delivered += 1;
      if (reminder.repeat) {
        store.reschedule(reminder.id, nextOccurrence(reminder.repeat, now, reminder.timeZone));
      } else {
        store.remove(reminder.id);
      }
    } catch (err) {
      const attempts = store.recordFailure(reminder.id, now + RETRY_DELAY_MS);
      console.error(`Failed to deliver reminder ${reminder.id} (attempt ${attempts}):`, err);
      if (attempts >= MAX_ATTEMPTS) {
        console.error(`Dropping reminder ${reminder.id} after ${attempts} failed attempts`);
        store.remove(reminder.id);
      }
    }
  }
  return delivered;
}
