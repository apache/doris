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

import { setCsrfToken } from './csrf';
import { fetchMe } from './me';

describe('fetchMe', () => {
  afterEach(() => setCsrfToken(null));

  it('keeps the server CSRF token only in memory', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          data: {
            user: 'analyst',
            capabilities: ['PLAYGROUND_USE', 'QUERY_PROFILE_VIEW_OWN'],
            csrfToken: 'csrf-session-token',
          },
          requestId: 'req-me',
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      ),
    );

    await expect(fetchMe()).resolves.toMatchObject({ user: 'analyst', csrfToken: 'csrf-session-token' });
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });
});
