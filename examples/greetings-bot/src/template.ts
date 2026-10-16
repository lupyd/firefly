export const DEFAULT_WELCOME = 'Welcome {user} to {group}! 👋';
export const DEFAULT_DM = 'Hi {user}, welcome to {group}! Say hello in the group when you are ready.';

export const MAX_TEMPLATE_LENGTH = 500;

// Fills {user} (as an @mention) and {group}; unknown placeholders are left as typed
export function renderTemplate(template: string, values: { user: string; group: string }): string {
  return template
    .replace(/\{user\}/g, `@${values.user}`)
    .replace(/\{group\}/g, values.group);
}

export function validateTemplate(template: string): string | null {
  const trimmed = template.trim();
  if (!trimmed) return 'The message cannot be empty.';
  if (trimmed.length > MAX_TEMPLATE_LENGTH) return `Keep the message under ${MAX_TEMPLATE_LENGTH} characters.`;
  return null;
}
