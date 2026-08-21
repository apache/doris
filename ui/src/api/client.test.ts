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

import { getCsrfToken, setCsrfToken } from './csrf';
import { UiApiError, uiRequest } from './client';

function jsonResponse(body: unknown, status = 200, requestId = 'req-test') {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'X-Request-Id': requestId,
    },
  });
}

describe('uiRequest', () => {
  afterEach(() => setCsrfToken(null));

  it('returns a direct typed response', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      jsonResponse({ user: 'root' }),
    );

    await expect(uiRequest<{ user: string }>('/rest/v1/ui/me')).resolves.toEqual({ user: 'root' });
  });

  it('unwraps the previous response during a rolling FE replacement', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      jsonResponse({ data: { user: 'root' }, requestId: 'req-body' }),
    );
    await expect(uiRequest<{ user: string }>('/rest/v1/ui/me')).resolves.toEqual({ user: 'root' });
  });

  it('adds a CSRF header to mutation requests without persisting the token', async () => {
    setCsrfToken('csrf-test');
    expect(getCsrfToken()).toBe('csrf-test');
    const fetchSpy = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(jsonResponse({ data: {}, requestId: 'req-test' }));

    await uiRequest('/rest/v1/ui/example', { method: 'POST', body: '{}' });

    const init = fetchSpy.mock.calls[0]?.[1];
    const headers = new Headers(init?.headers);
    expect(headers.get('X-Doris-CSRF-Token')).toBe('csrf-test');
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
  });

  it('normalizes a non-JSON unauthorized response and announces session expiry', async () => {
    setCsrfToken('expired-token');
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('Unauthorized', {
        status: 401,
        headers: { 'Content-Type': 'text/plain', 'X-Request-Id': 'req-401' },
      }),
    );
    const listener = vi.fn();
    window.addEventListener('doris-ui:unauthorized', listener);

    await expect(uiRequest('/rest/v1/ui/me')).rejects.toMatchObject({
      status: 401,
      code: 'UI_UNAUTHENTICATED',
      requestId: 'req-401',
    });
    expect(listener).toHaveBeenCalledOnce();
    expect(getCsrfToken()).toBeNull();
  });

  it.each([
    [400, 'UI_BAD_REQUEST'],
    [403, 'UI_FORBIDDEN'],
    [404, 'UI_NOT_FOUND'],
    [409, 'UI_CONFLICT'],
    [413, 'UI_PAYLOAD_TOO_LARGE'],
    [429, 'UI_RATE_LIMITED'],
    [500, 'UI_SERVER_ERROR'],
    [503, 'UI_SERVER_ERROR'],
  ])('normalizes an unstructured HTTP %s response', async (status, code) => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('failure', {
        status,
        headers: { 'Content-Type': 'text/plain', 'X-Request-Id': `req-${status}` },
      }),
    );

    await expect(uiRequest('/rest/v1/ui/example')).rejects.toMatchObject({
      status,
      code,
      requestId: `req-${status}`,
    });
  });

  it('normalizes invalid JSON without exposing a parser exception', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response('{', {
        status: 500,
        headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-json' },
      }),
    );

    await expect(uiRequest('/rest/v1/ui/example')).rejects.toMatchObject({
      status: 500,
      code: 'UI_INVALID_RESPONSE',
      requestId: 'req-json',
    });
  });

  it('rejects paths outside the UI API namespace', async () => {
    await expect(uiRequest('/rest/v1/session')).rejects.toThrow(
      'uiRequest only accepts UI bootstrap and Web SQL paths.',
    );
  });

  it('preserves a structured server error', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      jsonResponse(
        {
          code: 'UI_FORBIDDEN',
          message: 'You do not have permission to perform this operation.',
          requestId: 'req-403',
          details: { capability: 'CONFIGURATION_MODIFY' },
        },
        403,
      ),
    );

    try {
      await uiRequest('/rest/v1/ui/example');
      throw new Error('Expected uiRequest to fail.');
    } catch (error) {
      expect(error).toBeInstanceOf(UiApiError);
      expect(error).toMatchObject({ status: 403, code: 'UI_FORBIDDEN', requestId: 'req-403' });
    }
  });
});
