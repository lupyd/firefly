import * as path from 'path';
import { FireflyBot, BotContext } from 'firefly-client-js';
import { SqliteBotStorage } from '../../bot-storage/sqlite-storage';
import { SettingsStore } from './settings';
import { renderTemplate, validateTemplate } from './template';

// Keys and group state, so the bot keeps its device and groups across restarts
const state = new SqliteBotStorage(process.env.BOT_DB_FILE || path.resolve(process.cwd(), 'greetings-state.db'));
const bot = new FireflyBot({
  username: process.env.BOT_USERNAME || 'lupyd_welcome',
  sessionFile: process.env.BOT_SESSION_FILE || path.resolve(process.cwd(), 'greetings-session.json'),
  storage: state.providers,
});
const settings = new SettingsStore(process.env.GREETINGS_DB || path.resolve(process.cwd(), 'greetings-settings.db'));

const HELP = [
  'Welcome bot commands (admins only, except show and help):',
  '  /welcome show - Show the current welcome settings',
  '  /welcome on | off - Turn welcome messages on or off',
  '  /welcome set <message> - Change the message. Use {user} and {group}',
  '  /welcome channel - Post welcomes in this channel',
  '  /welcome dm on | off - Also send new members a direct message',
  '  /welcome dm set <message> - Change the direct message',
  '  /welcome test - Preview the welcome with yourself as the new member',
].join('\n');

async function groupName(groupId: number): Promise<string> {
  const groups = await bot.getGroups();
  return groups.find((g: { id: number }) => Number(g.id) === groupId)?.name || 'the group';
}

async function channelName(groupId: number, channelId: number | null): Promise<string> {
  const channels = await bot.getGroupChannels(groupId);
  const channel = channelId === null ? channels[0] : channels.find((c) => c.id === channelId);
  return channel ? `#${channel.name}` : 'the first channel';
}

// Text after "/welcome set" or "/welcome dm set", keeping the admin's line breaks
function messageAfterSet(text: string): string {
  return text.replace(/^\/welcome\s+(dm\s+)?set\s*/i, '');
}

bot.onMemberJoin(async (ctx) => {
  const config = settings.get(ctx.groupId);
  if (!config.enabled) return;

  const values = { user: ctx.username, group: await groupName(ctx.groupId) };
  await ctx.sendToGroup(renderTemplate(config.template, values), config.channelId ?? undefined);
  if (config.dmEnabled) {
    await ctx.sendDirect(renderTemplate(config.dmTemplate, values));
  }
});

bot.command('welcome', async (ctx: BotContext) => {
  if (!ctx.isGroup || ctx.groupId === null) {
    await ctx.reply('Add me to a group to welcome its new members. Then run /welcome help inside the group.');
    return;
  }
  const groupId = ctx.groupId;
  const [action = 'show', option] = ctx.args.map((a) => a.toLowerCase());

  if (action === 'help') {
    await ctx.reply(HELP);
    return;
  }
  if (action === 'show') {
    const config = settings.get(groupId);
    await ctx.reply([
      `Welcome messages: ${config.enabled ? 'on' : 'off'}, posted in ${await channelName(groupId, config.channelId)}`,
      `Message: ${config.template}`,
      `Direct message: ${config.dmEnabled ? config.dmTemplate : 'off'}`,
    ].join('\n'));
    return;
  }

  if (!(await bot.isGroupAdmin(groupId, ctx.sender))) {
    await ctx.reply('Only group admins can change welcome settings.');
    return;
  }

  switch (action) {
    case 'on':
    case 'off':
      settings.update(groupId, { enabled: action === 'on' });
      await ctx.reply(`Welcome messages are now ${action}.`);
      return;
    case 'set': {
      const message = messageAfterSet(ctx.text);
      const error = validateTemplate(message);
      if (error) {
        await ctx.reply(`${error} Example: /welcome set Welcome {user} to {group}!`);
        return;
      }
      settings.update(groupId, { template: message.trim() });
      await ctx.reply('Welcome message updated. Try it with /welcome test.');
      return;
    }
    case 'channel':
      settings.update(groupId, { channelId: ctx.channelId ?? 0 });
      await ctx.reply('New members will be welcomed in this channel.');
      return;
    case 'dm':
      if (option === 'on' || option === 'off') {
        settings.update(groupId, { dmEnabled: option === 'on' });
        await ctx.reply(`Welcome direct messages are now ${option}.`);
      } else if (option === 'set') {
        const message = messageAfterSet(ctx.text);
        const error = validateTemplate(message);
        if (error) {
          await ctx.reply(`${error} Example: /welcome dm set Hi {user}, the rules are pinned in #rules.`);
          return;
        }
        settings.update(groupId, { dmTemplate: message.trim() });
        await ctx.reply('Welcome direct message updated.');
      } else {
        await ctx.reply('Use /welcome dm on, /welcome dm off or /welcome dm set <message>.');
      }
      return;
    case 'test': {
      const config = settings.get(groupId);
      const values = { user: ctx.sender, group: await groupName(groupId) };
      await ctx.reply(`Preview: ${renderTemplate(config.template, values)}`);
      if (config.dmEnabled) {
        await bot.sendUserMessage(ctx.sender, `Preview: ${renderTemplate(config.dmTemplate, values)}`);
      }
      return;
    }
    default:
      await ctx.reply(HELP);
  }
});

bot.command('help', async (ctx: BotContext) => {
  await ctx.reply(HELP);
});

process.on('SIGINT', async () => {
  await bot.dispose();
  settings.close();
  state.close();
  process.exit(0);
});

await bot.start();
// start() logs connection errors instead of throwing; exit so a supervisor can retry
if (!bot.client?.isInitialized()) {
  console.error('Could not connect to Lupyd. Exiting.');
  process.exit(1);
}
