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
import {
  cancelWebSql,
  closeWebSqlSession,
  createWebSqlSession,
  executeWebSql,
  getWebSqlSession,
  resetWebSqlSession,
} from './webSql';

function json(data: unknown) {
  return new Response(JSON.stringify({ data, requestId: 'req-sql' }), {
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-sql' },
  });
}

describe('Web SQL API', () => {
  it('uses the session lifecycle endpoints and sends SQL as JSON', async () => {
    setCsrfToken('csrf-sql');
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(json({ sessionId: 'fe.session/one', createdAtMillis: 1, lastAccessMillis: 1 }))
      .mockResolvedValueOnce(json({ sessionId: 'fe.session/one', createdAtMillis: 1, lastAccessMillis: 1 }))
      .mockResolvedValueOnce(json({ columns: [], rows: [], affectedRows: 0, elapsedTimeMs: 2, queryId: null, warnings: [], catalog: 'internal', database: null, truncated: false }))
      .mockResolvedValueOnce(json({ cancelRequested: true }))
      .mockResolvedValueOnce(json({ sessionId: 'fe.session/one', createdAtMillis: 1, lastAccessMillis: 2 }))
      .mockResolvedValueOnce(json({ closed: true }));

    await createWebSqlSession();
    await getWebSqlSession('fe.session/one');
    await executeWebSql('fe.session/one', 'SELECT 1');
    await cancelWebSql('fe.session/one');
    await resetWebSqlSession('fe.session/one');
    await closeWebSqlSession('fe.session/one', true);

    expect(fetchSpy.mock.calls.map(([path]) => path)).toEqual([
      '/rest/v1/sql-sessions',
      '/rest/v1/sql-sessions/fe.session%2Fone',
      '/rest/v1/sql-sessions/fe.session%2Fone/statements',
      '/rest/v1/sql-sessions/fe.session%2Fone/cancel',
      '/rest/v1/sql-sessions/fe.session%2Fone/reset',
      '/rest/v1/sql-sessions/fe.session%2Fone',
    ]);
    const validationRequest = fetchSpy.mock.calls[1][1] as RequestInit;
    expect(validationRequest.method).toBe('GET');
    expect(new Headers(validationRequest.headers).has('X-Doris-CSRF-Token')).toBe(false);
    const statementRequest = fetchSpy.mock.calls[2][1] as RequestInit;
    expect(statementRequest.body).toBe(JSON.stringify({ sql: 'SELECT 1' }));
    expect(new Headers(statementRequest.headers).get('X-Doris-CSRF-Token')).toBe('csrf-sql');
    expect((fetchSpy.mock.calls[5][1] as RequestInit).keepalive).toBe(true);
  });
});
