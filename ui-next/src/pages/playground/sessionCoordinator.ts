// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

export const WEB_SQL_SESSION_STORAGE_KEY = 'doris.ui.web-sql-session.v1';
const CHANNEL_NAME = 'doris.ui.web-sql-session-claims.v1';

type ClaimMessage =
  | { type: 'probe'; sessionId: string; nonce: string }
  | { type: 'claimed'; sessionId: string; nonce: string };

interface CoordinationCrypto {
  randomUUID?: () => string;
  getRandomValues?: (values: Uint8Array) => Uint8Array;
}

let fallbackNonceSequence = 0;

export function createCoordinationNonce(
  source: CoordinationCrypto | null | undefined = globalThis.crypto,
): string {
  if (typeof source?.randomUUID === 'function') {
    try {
      return source.randomUUID.call(source);
    } catch {
      // Continue to the broadly available getRandomValues fallback.
    }
  }

  if (typeof source?.getRandomValues === 'function') {
    try {
      const values = source.getRandomValues.call(source, new Uint8Array(16));
      return Array.from(values, (value) => value.toString(16).padStart(2, '0')).join('');
    } catch {
      // This nonce is only a BroadcastChannel correlation id, not a credential.
    }
  }

  fallbackNonceSequence += 1;
  return `local-${Date.now().toString(36)}-${fallbackNonceSequence.toString(36)}-${Math.random().toString(36).slice(2)}`;
}

export function storedSessionId(): string | null {
  return sessionStorage.getItem(WEB_SQL_SESSION_STORAGE_KEY);
}

export function storeSessionId(sessionId: string | null): void {
  if (sessionId) sessionStorage.setItem(WEB_SQL_SESSION_STORAGE_KEY, sessionId);
  else sessionStorage.removeItem(WEB_SQL_SESSION_STORAGE_KEY);
}

export async function isClaimedByAnotherTab(sessionId: string, timeoutMs = 80): Promise<boolean> {
  if (typeof globalThis.BroadcastChannel !== 'function') return false;

  let channel: BroadcastChannel;
  try {
    channel = new BroadcastChannel(CHANNEL_NAME);
  } catch {
    // If coordination is unavailable, avoid adopting a session that a copied
    // tab may already be using. The caller will create an independent session.
    return true;
  }
  const nonce = createCoordinationNonce();
  return new Promise((resolve) => {
    let claimed = false;
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      channel.close();
      resolve(claimed);
    };
    channel.onmessage = (event: MessageEvent<ClaimMessage>) => {
      const message = event.data;
      if (message && message.type === 'claimed'
        && message.sessionId === sessionId && message.nonce === nonce) {
        claimed = true;
        finish();
      }
    };
    try {
      channel.postMessage({ type: 'probe', sessionId, nonce } satisfies ClaimMessage);
    } catch {
      // Conservatively replace the stored session if cross-tab coordination
      // fails after the channel was opened.
      claimed = true;
      finish();
      return;
    }
    window.setTimeout(() => {
      if (!claimed) finish();
    }, timeoutMs);
  });
}

export function claimSessionForTab(sessionId: string): () => void {
  if (typeof globalThis.BroadcastChannel !== 'function') return () => undefined;
  let channel: BroadcastChannel;
  try {
    channel = new BroadcastChannel(CHANNEL_NAME);
  } catch {
    return () => undefined;
  }
  channel.onmessage = (event: MessageEvent<ClaimMessage>) => {
    const message = event.data;
    if (message.type === 'probe' && message.sessionId === sessionId) {
      channel.postMessage({ type: 'claimed', sessionId, nonce: message.nonce } satisfies ClaimMessage);
    }
  };
  return () => channel.close();
}
