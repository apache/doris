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

import '@testing-library/jest-dom/vitest';

// Node 22.4 and newer install experimental `localStorage`/`sessionStorage` accessors that stay
// undefined unless the process was started with --localstorage-file, and they shadow jsdom's own
// implementation. Anything touching storage therefore breaks on a Node newer than the 22.12 floor.
// Substitute an in-memory Storage when the global is unusable; on Node 22 jsdom's is left alone.
function installStorage(name: 'localStorage' | 'sessionStorage'): void {
  try {
    if (window[name]) return;
  } catch {
    // An unusable accessor throws; fall through and replace it.
  }
  const entries = new Map<string, string>();
  const storage = {
    get length(): number {
      return entries.size;
    },
    key: (index: number): string | null => Array.from(entries.keys())[index] ?? null,
    getItem: (key: string): string | null => entries.get(String(key)) ?? null,
    setItem: (key: string, value: string): void => {
      entries.set(String(key), String(value));
    },
    removeItem: (key: string): void => {
      entries.delete(String(key));
    },
    clear: (): void => {
      entries.clear();
    },
  };
  Object.defineProperty(globalThis, name, { configurable: true, writable: true, value: storage });
}

installStorage('localStorage');
installStorage('sessionStorage');

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

Object.defineProperty(globalThis, 'ResizeObserver', {
  writable: true,
  value: ResizeObserverMock,
});
