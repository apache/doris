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

import {
  createCoordinationNonce,
  WEB_SQL_SESSION_STORAGE_KEY,
  storeSessionId,
  storedSessionId,
} from './sessionCoordinator';

describe('Playground session storage', () => {
  it('stores only the opaque session id in sessionStorage and clears it', () => {
    storeSessionId('fe-hint.random-id');
    expect(storedSessionId()).toBe('fe-hint.random-id');
    expect(sessionStorage).toHaveLength(1);
    expect(sessionStorage.getItem(WEB_SQL_SESSION_STORAGE_KEY)).toBe('fe-hint.random-id');

    storeSessionId(null);
    expect(storedSessionId()).toBeNull();
  });

  it('uses randomUUID when the browser provides it', () => {
    expect(createCoordinationNonce({ randomUUID: () => 'uuid-nonce' })).toBe('uuid-nonce');
  });

  it('uses getRandomValues when randomUUID is unavailable on remote HTTP', () => {
    const nonce = createCoordinationNonce({
      getRandomValues: (values) => {
        values.forEach((_, index) => { values[index] = index; });
        return values;
      },
    });

    expect(nonce).toBe('000102030405060708090a0b0c0d0e0f');
  });

  it('uses a unique local correlation id in very old browsers', () => {
    const first = createCoordinationNonce(null);
    const second = createCoordinationNonce(null);

    expect(first).toMatch(/^local-/);
    expect(second).toMatch(/^local-/);
    expect(second).not.toBe(first);
  });
});
