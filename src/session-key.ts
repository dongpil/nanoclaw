import { getRegistrationJid } from './conversation-jid.js';

/**
 * Root channel conversations keep the legacy per-group session key.
 * Sub-conversations (e.g. Slack threads) get isolated session keys.
 */
export function getSessionKey(groupFolder: string, chatJid: string): string {
  const registrationJid = getRegistrationJid(chatJid);
  if (chatJid === registrationJid) return groupFolder;
  return `chat:${chatJid}`;
}
