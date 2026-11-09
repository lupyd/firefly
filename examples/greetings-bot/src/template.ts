export const DEFAULT_WELCOME = 'Welcome {user} to {group}! 👋';
export const DEFAULT_DM = 'Hi {user}, welcome to {group}! Say hello in the group when you are ready.';

export const MAX_TEMPLATE_LENGTH = 500;

// Fills {user} (as an @mention) and {group}; unknown placeholders are left as typed
export function renderTemplate(template: string, values: { user: string; group: string }): string {
  return template
    .replace(/\{user\}/g, `@${values.user}`)
    .replace(/\{group\}/g, values.group);
}

// Direct messages come from the bot's own account but are written by a group's admins, so they
// always say who wrote them; otherwise any admin could speak privately to newcomers as the bot.
export function renderDirectMessage(template: string, values: { user: string; group: string }): string {
  return `From the admins of ${values.group}:\n${renderTemplate(template, values)}`;
}

export function validateTemplate(template: string): string | null {
  const trimmed = template.trim();
  if (!trimmed) return 'The message cannot be empty.';
  if (trimmed.length > MAX_TEMPLATE_LENGTH) return `Keep the message under ${MAX_TEMPLATE_LENGTH} characters.`;
  return null;
}
