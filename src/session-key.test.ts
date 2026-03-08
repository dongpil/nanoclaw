import { describe, expect, it } from 'vitest';

import { getSessionKey } from './session-key.js';

describe('getSessionKey', () => {
  it('uses group folder for root channel conversations', () => {
    expect(getSessionKey('slack_main', 'slack:C123')).toBe('slack_main');
  });

  it('uses thread-scoped key for Slack threads', () => {
    expect(
      getSessionKey('slack_main', 'slack:C123::thread:1704067200.000000'),
    ).toBe('chat:slack:C123::thread:1704067200.000000');
  });
});
