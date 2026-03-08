const SLACK_PREFIX = 'slack:';
const SLACK_THREAD_DELIMITER = '::thread:';

export interface ParsedSlackConversationJid {
  baseJid: string;
  threadTs?: string;
}

export function buildSlackConversationJid(
  channelId: string,
  threadTs?: string,
): string {
  const baseJid = `${SLACK_PREFIX}${channelId}`;
  if (!threadTs || threadTs.trim() === '') return baseJid;
  return `${baseJid}${SLACK_THREAD_DELIMITER}${threadTs}`;
}

export function parseSlackConversationJid(
  jid: string,
): ParsedSlackConversationJid | null {
  if (!jid.startsWith(SLACK_PREFIX)) return null;

  const delimiterIndex = jid.indexOf(SLACK_THREAD_DELIMITER);
  if (delimiterIndex === -1) {
    return { baseJid: jid };
  }

  const baseJid = jid.slice(0, delimiterIndex);
  const threadTs = jid.slice(delimiterIndex + SLACK_THREAD_DELIMITER.length);
  if (!threadTs) return { baseJid };
  return { baseJid, threadTs };
}

/**
 * Map a conversation JID to the registration key.
 * Slack thread conversations inherit registration from their parent channel.
 */
export function getRegistrationJid(chatJid: string): string {
  const parsedSlack = parseSlackConversationJid(chatJid);
  return parsedSlack?.baseJid ?? chatJid;
}

export function getSlackThreadLikePattern(baseJid: string): string | null {
  if (!baseJid.startsWith(SLACK_PREFIX)) return null;
  return `${baseJid}${SLACK_THREAD_DELIMITER}%`;
}
