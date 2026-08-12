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
import { UiApiError, uiRequest } from './client';
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

export async function login(username: string, password: string): Promise<UiMe> {
  let response: Response;
  try {
    response = await fetch('/rest/v1/ui/login', {
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
    if (!response.ok) throw loginError(response.status);
  }
  if (!response.ok) throw loginError(response.status, payload);
  if (!payload || typeof payload !== 'object' || !('data' in payload)) throw loginError(response.status);
  const me = (payload as { data: UiMe }).data;
  setCsrfToken(me.csrfToken);
  return me;
}

export async function logout(): Promise<void> {
  await uiRequest<{ loggedOut: boolean }>('/rest/v1/ui/logout', { method: 'POST' });
  setCsrfToken(null);
}
