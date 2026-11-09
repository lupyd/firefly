import * as path from 'path';
import { FireflyBot, BotContext } from 'firefly-client-js';
import { SqliteBotStorage } from '../../bot-storage/sqlite-storage';
import { describeRepeat, parseRemind } from './parse';
import { deliverDue } from './scheduler';
import { ReminderStore, type NewReminder, type Reminder } from './store';
import { formatInZone, isValidTimeZone } from './time';

const MAX_REMINDERS_PER_USER = 25;
const TICK_MS = 15_000;

// Keys and group state, so the bot keeps its device and groups across restarts
const state = new SqliteBotStorage(process.env.BOT_DB_FILE || path.resolve(process.cwd(), 'reminder-state.db'));
const bot = new FireflyBot({
  username: process.env.BOT_USERNAME || 'reminder_bot',
  sessionFile: process.env.BOT_SESSION_FILE || path.resolve(process.cwd(), 'reminder-session.json'),
  storage: state.providers,
});
const store = new ReminderStore(process.env.REMINDERS_DB || path.resolve(process.cwd(), 'reminders.db'));

const HELP = [
  'Reminder commands:',
  '  /remind me in 2h check the deploy',
  '  /remind me tomorrow at 9am send the report',
  '  /remind here every monday at 10am weekly sync',
  '  /remind @bob in 30m review my PR (inside a group)',
  '  /reminders - List your reminders',
  '  /remind cancel <id> - Cancel one',
  '  /timezone Asia/Kolkata - Set your time zone (default UTC)',
  '',
  '"me" reminders arrive as a direct message. "here" and @user reminders are posted where you set them.',
  'Times: in 10m / in 1h30m / at 5pm / today 18:00 / tomorrow / on friday / on 2026-12-24 at 10am / every day / every weekday',
].join('\n');

async function groupNames(): Promise<Map<number, string>> {
  const groups: Array<{ id: number; name: string }> = await bot.getGroups();
  return new Map(groups.map((g) => [Number(g.id), g.name]));
}

function describeDelivery(reminder: Reminder, names: Map<number, string>): string {
  if (reminder.dmTo) return 'by direct message';
  const group = names.get(reminder.groupId ?? -1) ?? 'a group';
  return reminder.mention === reminder.creator ? `in ${group}` : `for @${reminder.mention} in ${group}`;
}

async function listReminders(ctx: BotContext): Promise<void> {
  const reminders = store.listByCreator(ctx.sender);
  if (reminders.length === 0) {
    await ctx.reply('You have no reminders. Set one with /remind me in 1h stretch.');
    return;
  }
  const names = await groupNames();
  const lines = reminders.map((r) => {
    const when = r.repeat ? describeRepeat(r.repeat) : formatInZone(r.dueAt, r.timeZone);
    return `#${r.id} · ${when} · ${describeDelivery(r, names)} · ${r.text}`;
  });
  await ctx.reply(`Your reminders (${store.getTimeZone(ctx.sender)}):\n${lines.join('\n')}`);
}

bot.command('remind', async (ctx: BotContext) => {
  const [first, second] = ctx.args.map((a) => a.toLowerCase());
  if (!first || first === 'help') {
    await ctx.reply(HELP);
    return;
  }
  if (first === 'list') {
    await listReminders(ctx);
    return;
  }
  if (first === 'cancel' || first === 'delete') {
    const id = Number((second ?? '').replace(/^#/, ''));
    if (!Number.isInteger(id) || id <= 0) {
      await ctx.reply('Tell me which one, for example: /remind cancel 3. See the ids with /reminders.');
      return;
    }
    await ctx.reply(store.delete(id, ctx.sender) ? `Cancelled reminder #${id}.` : `You have no reminder #${id}.`);
    return;
  }

  const timeZone = store.getTimeZone(ctx.sender);
  const parsed = parseRemind(ctx.args, Date.now(), timeZone);
  if (!parsed.ok) {
    await ctx.reply(`${parsed.error}\nSee /remind help for examples.`);
    return;
  }
  const { target, dueAt, repeat, text, relative } = parsed.reminder;

  if (target.kind === 'user' && !ctx.isGroup) {
    await ctx.reply('Reminders for other people only work inside a group, so they see who set them.');
    return;
  }
  if (store.countByCreator(ctx.sender) >= MAX_REMINDERS_PER_USER) {
    await ctx.reply(`You already have ${MAX_REMINDERS_PER_USER} reminders. Cancel some with /remind cancel <id>.`);
    return;
  }

  // "me" goes by DM; "here" and @user stay where the command was sent
  const inGroup = target.kind !== 'me' && ctx.isGroup && ctx.groupId !== null;
  const reminder: NewReminder = {
    creator: ctx.sender,
    groupId: inGroup ? ctx.groupId : null,
    channelId: inGroup ? ctx.channelId ?? 0 : null,
    dmTo: inGroup ? null : ctx.sender,
    mention: target.kind === 'user' ? target.username : ctx.sender,
    text,
    dueAt,
    repeat,
    timeZone,
  };
  const id = store.add(reminder);

  const when = repeat ? describeRepeat(repeat) : formatInZone(dueAt, timeZone);
  const where = inGroup ? 'here' : 'by direct message';
  const lines = [`Got it (#${id}). I'll remind ${target.kind === 'user' ? `@${target.username}` : 'you'} ${where}, ${when} (${timeZone}).`];
  if (!relative && !store.hasTimeZone(ctx.sender)) {
    lines.push('Tip: times are in UTC until you set your time zone, e.g. /timezone Asia/Kolkata');
  }
  await ctx.reply(lines.join('\n'));
});

bot.command('reminders', listReminders);

bot.command('timezone', async (ctx: BotContext) => {
  const timeZone = ctx.args[0];
  if (!timeZone) {
    await ctx.reply(`Your time zone is ${store.getTimeZone(ctx.sender)}. Change it with /timezone Europe/London.`);
    return;
  }
  if (!isValidTimeZone(timeZone)) {
    await ctx.reply(`"${timeZone}" is not a time zone I know. Use a name like America/New_York, Asia/Kolkata or UTC.`);
    return;
  }
  store.setTimeZone(ctx.sender, timeZone);
  await ctx.reply(`Time zone set to ${timeZone}. Reminders you already set keep their original time.`);
});

bot.command('help', async (ctx: BotContext) => {
  await ctx.reply(HELP);
});

async function send(reminder: Reminder, message: string): Promise<void> {
  if (reminder.dmTo) {
    await bot.sendUserMessage(reminder.dmTo, message);
  } else {
    await bot.sendGroupMessage(reminder.groupId!, message, reminder.channelId ?? 0);
  }
}

let ticking = false;
async function tick(): Promise<void> {
  if (ticking || !bot.client) return;
  ticking = true;
  try {
    await deliverDue(store, Date.now(), send);
  } catch (err) {
    console.error('Reminder tick failed:', err);
  } finally {
    ticking = false;
  }
}

process.on('SIGINT', async () => {
  await bot.dispose();
  store.close();
  state.close();
  process.exit(0);
});

await bot.start();
// start() logs connection errors instead of throwing; exit so a supervisor can retry
if (!bot.client?.isInitialized()) {
  console.error('Could not connect to Lupyd. Exiting.');
  process.exit(1);
}
setInterval(tick, TICK_MS);
tick();
