import { describe, expect, it } from 'vitest';

import {
  buildSlackConversationJid,
  getRegistrationJid,
  getSlackThreadLikePattern,
  parseSlackConversationJid,
} from './conversation-jid.js';

describe('conversation-jid', () => {
  it('builds and parses Slack thread JIDs', () => {
    const jid = buildSlackConversationJid('C123', '1704067200.000000');
    expect(jid).toBe('slack:C123::thread:1704067200.000000');
    expect(parseSlackConversationJid(jid)).toEqual({
      baseJid: 'slack:C123',
      threadTs: '1704067200.000000',
    });
  });

  it('maps Slack thread JID to registration JID', () => {
    expect(getRegistrationJid('slack:C123::thread:1704067200.000000')).toBe(
      'slack:C123',
    );
  });

  it('returns LIKE pattern for Slack base JID only', () => {
    expect(getSlackThreadLikePattern('slack:C123')).toBe(
      'slack:C123::thread:%',
    );
    expect(getSlackThreadLikePattern('tg:123')).toBeNull();
  });
});
