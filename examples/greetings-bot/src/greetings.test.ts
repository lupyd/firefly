import { describe, expect, test } from 'bun:test';
import { SettingsStore } from './settings';
import { DEFAULT_WELCOME, MAX_TEMPLATE_LENGTH, renderTemplate, validateTemplate } from './template';

describe('renderTemplate', () => {
  test('mentions the user and names the group', () => {
    expect(renderTemplate('Welcome {user} to {group}!', { user: 'alice', group: 'Design' }))
      .toBe('Welcome @alice to Design!');
  });

  test('replaces every placeholder and keeps unknown ones', () => {
    expect(renderTemplate('{user} {user} {rules}', { user: 'bob', group: 'G' })).toBe('@bob @bob {rules}');
  });
});

describe('validateTemplate', () => {
  test('rejects empty and overly long messages', () => {
    expect(validateTemplate('   ')).not.toBeNull();
    expect(validateTemplate('x'.repeat(MAX_TEMPLATE_LENGTH + 1))).not.toBeNull();
    expect(validateTemplate('Hi {user}')).toBeNull();
  });
});

describe('SettingsStore', () => {
  test('returns defaults for unknown groups', () => {
    const store = new SettingsStore(':memory:');
    expect(store.get(7)).toMatchObject({ enabled: true, channelId: null, template: DEFAULT_WELCOME, dmEnabled: false });
    store.close();
  });

  test('persists partial updates without losing other fields', () => {
    const store = new SettingsStore(':memory:');
    store.update(7, { template: 'Hello {user}' });
    store.update(7, { channelId: 3, dmEnabled: true });
    expect(store.get(7)).toMatchObject({ enabled: true, channelId: 3, template: 'Hello {user}', dmEnabled: true });
    expect(store.get(8).template).toBe(DEFAULT_WELCOME);
    store.close();
  });
});
