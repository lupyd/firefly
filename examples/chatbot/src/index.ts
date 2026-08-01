import { FireflyBot, BotContext } from 'firefly-client-js';

// Initialize the bot with defaults (automatically reads EMULATOR_MODE and environment variables)
const bot = new FireflyBot();

// Register chatbot commands

// /hi - Greets the user
bot.command('hi', async (ctx: BotContext) => {
  const greeting = `Hello, @${ctx.sender}! I am the Firefly MLS Example Chatbot. Nice to meet you!`;
  await ctx.reply(greeting);
});

// /joke - Tells a dad joke
bot.command('joke', async (ctx: BotContext) => {
  const joke = await fetchDadJoke();
  await ctx.reply(joke);
});

// /help - Lists commands
bot.command('help', async (ctx: BotContext) => {
  const helpText = 
    `Available Commands:\n` +
    `  /hi   - Greet the bot\n` +
    `  /joke - Get a funny dad joke\n` +
    `  /help - Show this message`;
  await ctx.reply(helpText);
});

// Helper: Fetch a dad joke from public API
async function fetchDadJoke(): Promise<string> {
  try {
    const response = await fetch('https://icanhazdadjoke.com/', {
      headers: { 'Accept': 'application/json' }
    });
    if (response.ok) {
      const data = await response.json() as { joke: string };
      return data.joke;
    }
  } catch (e: any) {
    console.error('Error fetching dad joke:', e.message);
  }
  return "I would tell you a joke about UDP, but you might not get it.";
}

// Start the bot connection and message loop
bot.start();
