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
import { login, logout } from './auth';

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-auth' },
  });
}

describe('authentication API', () => {
  afterEach(() => setCsrfToken(null));

  it('sends Basic credentials only to login and keeps them out of browser storage', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
      json({ code: 200, msg: 'Login success!' }),
    ).mockResolvedValueOnce(
      json({
        data: { user: 'root', csrfToken: 'csrf-login' },
        requestId: 'req-auth',
      }),
    );

    await login('root', 'secret');

    const loginHeaders = new Headers(fetchSpy.mock.calls[0]?.[1]?.headers);
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/rest/v1/login');
    expect(loginHeaders.get('Authorization')).toBe(`Basic ${btoa('root:secret')}`);
    expect(fetchSpy.mock.calls[1]?.[0]).toBe('/rest/v1/ui/me');
    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(getCsrfToken()).toBe('csrf-login');
  });

  it('maps invalid credentials without revealing whether the user exists', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(json({
      code: 401,
      msg: 'Unauthorized',
      data: 'Access denied for user missing',
    }));

    await expect(login('missing', 'wrong')).rejects.toMatchObject({
      status: 401,
      code: 'UI_LOGIN_FAILED',
      message: 'Sign-in failed. Check the username and password.',
    });
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it('preserves the dedicated error for an authenticated non-admin user', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValueOnce(
      json({ code: 200, msg: 'Login success!' }),
    ).mockResolvedValueOnce(
      json(
        {
          code: 'UI_ADMIN_REQUIRED',
          message: 'This account is authenticated but is not authorized to use the Doris Web Console.',
          requestId: 'req-auth',
        },
        403,
      ),
    ).mockResolvedValueOnce(json({ code: 0, data: {} }));

    await expect(login('analyst', 'secret')).rejects.toMatchObject({
      status: 403,
      code: 'UI_ADMIN_REQUIRED',
      message: 'This account is authenticated but is not authorized to use the Doris Web Console.',
    });
    expect(fetchSpy.mock.calls[2]?.[0]).toBe('/rest/v1/logout');
  });

  it('uses the existing logout endpoint and clears the UI token afterward', async () => {
    setCsrfToken('csrf-logout');
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      json({ data: { loggedOut: true }, requestId: 'req-auth' }),
    );

    await logout();

    const headers = new Headers(fetchSpy.mock.calls[0]?.[1]?.headers);
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/rest/v1/logout');
    expect(headers.get('X-Doris-CSRF-Token')).toBeNull();
    expect(getCsrfToken()).toBeNull();
  });
});
