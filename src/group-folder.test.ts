import path from 'path';

import { describe, expect, it } from 'vitest';

import {
  getConversationIpcKey,
  isValidGroupFolder,
  resolveConversationIpcInputPath,
  resolveGroupFolderPath,
  resolveGroupIpcPath,
} from './group-folder.js';

describe('group folder validation', () => {
  it('accepts normal group folder names', () => {
    expect(isValidGroupFolder('main')).toBe(true);
    expect(isValidGroupFolder('family-chat')).toBe(true);
    expect(isValidGroupFolder('Team_42')).toBe(true);
  });

  it('rejects traversal and reserved names', () => {
    expect(isValidGroupFolder('../../etc')).toBe(false);
    expect(isValidGroupFolder('/tmp')).toBe(false);
    expect(isValidGroupFolder('global')).toBe(false);
    expect(isValidGroupFolder('')).toBe(false);
  });

  it('resolves safe paths under groups directory', () => {
    const resolved = resolveGroupFolderPath('family-chat');
    expect(resolved.endsWith(`${path.sep}groups${path.sep}family-chat`)).toBe(
      true,
    );
  });

  it('resolves safe paths under data ipc directory', () => {
    const resolved = resolveGroupIpcPath('family-chat');
    expect(
      resolved.endsWith(`${path.sep}data${path.sep}ipc${path.sep}family-chat`),
    ).toBe(true);
  });

  it('throws for unsafe folder names', () => {
    expect(() => resolveGroupFolderPath('../../etc')).toThrow();
    expect(() => resolveGroupIpcPath('/tmp')).toThrow();
  });

  it('builds stable per-conversation ipc key', () => {
    const key1 = getConversationIpcKey('slack:C123::thread:1700000.000001');
    const key2 = getConversationIpcKey('slack:C123::thread:1700000.000001');
    const key3 = getConversationIpcKey('slack:C123::thread:1700000.000002');
    expect(key1).toBe(key2);
    expect(key1).not.toBe(key3);
  });

  it('resolves per-conversation input path under group ipc dir', () => {
    const resolved = resolveConversationIpcInputPath(
      'family-chat',
      'slack:C123::thread:1700000.000001',
    );
    expect(
      resolved.includes(`${path.sep}data${path.sep}ipc${path.sep}family-chat`),
    ).toBe(true);
    expect(resolved.includes(`${path.sep}input${path.sep}`)).toBe(true);
  });
});
