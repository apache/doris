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
import { UiApiError } from './client';
import { fetchMe } from './me';
import type { UiMe } from './types';

function encodeBasic(username: string, password: string): string {
  const bytes = new TextEncoder().encode(`${username}:${password}`);
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function loginError(status: number, body?: unknown): UiApiError {
  if (body && typeof body === 'object' && 'code' in body && 'message' in body && 'requestId' in body) {
    const error = body as { code: string; message: string; requestId: string; details?: unknown };
    return new UiApiError(status, error);
  }
  const requestId = 'unknown';
  if (status === 429) {
    return new UiApiError(status, {
      code: 'UI_RATE_LIMITED',
      message: 'Too many sign-in attempts. Please try again later.',
      requestId,
    });
  }
  if (status === 403) {
    return new UiApiError(status, {
      code: 'UI_ADMIN_REQUIRED',
      message: 'This account is authenticated but is not authorized to use the Doris Web Console.',
      requestId,
    });
  }
  if (status === 401) {
    return new UiApiError(status, {
      code: 'UI_LOGIN_FAILED',
      message: 'Sign-in failed. Check the username and password.',
      requestId,
    });
  }
  return new UiApiError(status, {
    code: 'UI_LOGIN_UNAVAILABLE',
    message: 'The Doris FE could not complete sign-in.',
    requestId,
  });
}

function legacyResponseCode(body: unknown): number | null {
  if (!body || typeof body !== 'object' || !('code' in body)) return null;
  const code = (body as { code?: unknown }).code;
  return typeof code === 'number' ? code : null;
}

export async function login(username: string, password: string): Promise<UiMe> {
  let response: Response;
  try {
    response = await fetch('/rest/v1/login', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        Authorization: `Basic ${encodeBasic(username, password)}`,
      },
      credentials: 'same-origin',
    });
  } catch {
    throw new UiApiError(0, {
      code: 'UI_LOGIN_UNAVAILABLE',
      message: 'The Doris FE is unavailable. Check the connection and try again.',
      requestId: 'unknown',
    });
  }

  let payload: unknown = null;
  try {
    payload = await response.json();
  } catch {
    throw loginError(response.ok ? 500 : response.status);
  }
  const bodyCode = legacyResponseCode(payload);
  const bodyRejected = bodyCode !== null && bodyCode !== 0 && bodyCode !== 200;
  if (!response.ok || bodyRejected) {
    throw loginError(bodyRejected ? bodyCode : response.status, payload);
  }
  try {
    return await fetchMe();
  } catch (error) {
    // The legacy login endpoint authenticates every valid Doris user. The UI
    // bootstrap performs the ADMIN check; remove the just-created cookie when
    // that second step rejects the account.
    try {
      await logout();
    } catch {
      setCsrfToken(null);
    }
    throw error;
  }
}

export async function logout(): Promise<void> {
  const response = await fetch('/rest/v1/logout', {
    method: 'POST',
    headers: { Accept: 'application/json' },
    credentials: 'same-origin',
  });
  if (!response.ok) throw loginError(response.status);
  setCsrfToken(null);
}
