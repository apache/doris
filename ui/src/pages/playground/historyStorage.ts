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

export const MAX_RESULT_TABS = 10;
// Keep persisted rows bounded so a large result cannot exhaust sessionStorage.
export const MAX_PERSISTED_ROWS = 200;
const MAX_MESSAGES = 100;
const HISTORY_PREFIX = 'doris.ui.web-sql-history.v1:';

export interface HistorySnapshot {
  results: unknown[];
  messages: unknown[];
  editorValue?: string;
}

function storageKey(sessionId: string): string {
  return `${HISTORY_PREFIX}${sessionId}`;
}

export function loadHistory(sessionId: string): HistorySnapshot | null {
  try {
    const raw = sessionStorage.getItem(storageKey(sessionId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<HistorySnapshot>;
    if (!Array.isArray(parsed.results) || !Array.isArray(parsed.messages)) return null;
    return {
      results: parsed.results,
      messages: parsed.messages,
      editorValue: typeof parsed.editorValue === 'string' ? parsed.editorValue : undefined,
    };
  } catch {
    return null;
  }
}

export function saveHistory(sessionId: string, value: HistorySnapshot): void {
  try {
    const key = storageKey(sessionId);
    for (let index = sessionStorage.length - 1; index >= 0; index -= 1) {
      const existing = sessionStorage.key(index);
      if (existing?.startsWith(HISTORY_PREFIX) && existing !== key) sessionStorage.removeItem(existing);
    }
    const results = value.results.slice(0, MAX_RESULT_TABS).map((result) => {
      if (!result || typeof result !== 'object') return result;
      const entry = result as { result?: { rows?: unknown[][] } };
      if (!entry.result || !Array.isArray(entry.result.rows)) return result;
      return { ...entry, result: { ...entry.result, rows: entry.result.rows.slice(0, MAX_PERSISTED_ROWS) } };
    });
    sessionStorage.setItem(key, JSON.stringify({
      results,
      messages: value.messages.slice(0, MAX_MESSAGES),
      editorValue: value.editorValue,
    }));
  } catch {
    // Storage can be unavailable or over quota, neither should break Playground.
  }
}

export function clearHistory(sessionId: string): void {
  try {
    sessionStorage.removeItem(storageKey(sessionId));
  } catch {
    // Private browsing modes may disable sessionStorage.
  }
}
