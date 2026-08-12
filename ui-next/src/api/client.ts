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
import type { UiErrorBody, UiSuccess } from './types';

const MUTATING_METHODS = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

export class UiApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId: string;
  readonly details?: unknown;

  constructor(status: number, body: UiErrorBody) {
    super(body.message);
    this.name = 'UiApiError';
    this.status = status;
    this.code = body.code;
    this.requestId = body.requestId;
    this.details = body.details;
  }
}

function isUiErrorBody(value: unknown): value is UiErrorBody {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<UiErrorBody>;
  return (
    typeof candidate.code === 'string' &&
    typeof candidate.message === 'string' &&
    typeof candidate.requestId === 'string'
  );
}

function fallbackError(status: number, requestId: string): UiErrorBody {
  const errors: Record<number, Pick<UiErrorBody, 'code' | 'message'>> = {
    400: { code: 'UI_BAD_REQUEST', message: 'The request is invalid.' },
    401: { code: 'UI_UNAUTHENTICATED', message: 'Authentication is required.' },
    403: { code: 'UI_FORBIDDEN', message: 'You do not have permission to perform this operation.' },
    404: { code: 'UI_NOT_FOUND', message: 'The requested resource was not found.' },
    409: { code: 'UI_CONFLICT', message: 'The request conflicts with the current state.' },
    413: { code: 'UI_PAYLOAD_TOO_LARGE', message: 'The request is too large.' },
    429: { code: 'UI_RATE_LIMITED', message: 'Too many requests. Please try again later.' },
  };
  const known = errors[status];
  if (known) return { ...known, requestId };
  if (status >= 500) {
    return { code: 'UI_SERVER_ERROR', message: 'The server could not complete the request.', requestId };
  }
  return {
    code: 'UI_REQUEST_FAILED',
    message: 'The request could not be completed.',
    requestId,
  };
}

export async function uiRequest<T>(path: string, init: RequestInit = {}): Promise<UiSuccess<T>> {
  if (!path.startsWith('/rest/v1/ui/')) {
    throw new Error('uiRequest only accepts /rest/v1/ui/ paths.');
  }

  const method = (init.method ?? 'GET').toUpperCase();
  const headers = new Headers(init.headers);
  headers.set('Accept', 'application/json');

  if (init.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  if (MUTATING_METHODS.has(method)) {
    const token = getCsrfToken();
    if (token) headers.set('X-Doris-CSRF-Token', token);
  }

  const response = await fetch(path, {
    ...init,
    method,
    headers,
    credentials: 'same-origin',
  });
  const requestId = response.headers.get('X-Request-Id') ?? 'unknown';
  const contentType = response.headers.get('Content-Type') ?? '';
  let payload: unknown = null;
  if (contentType.includes('application/json')) {
    try {
      payload = await response.json();
    } catch {
      throw new UiApiError(response.status, {
        code: 'UI_INVALID_RESPONSE',
        message: 'The server returned invalid JSON.',
        requestId,
      });
    }
  }

  if (!response.ok) {
    const error = isUiErrorBody(payload) ? payload : fallbackError(response.status, requestId);
    if (response.status === 401) {
      setCsrfToken(null);
      window.dispatchEvent(new CustomEvent('doris-ui:unauthorized'));
    }
    throw new UiApiError(response.status, error);
  }

  if (!payload || typeof payload !== 'object' || !('data' in payload)) {
    throw new UiApiError(response.status, {
      code: 'UI_INVALID_RESPONSE',
      message: 'The server returned an invalid response.',
      requestId,
    });
  }

  const success = payload as UiSuccess<T>;
  return { data: success.data, requestId: success.requestId || requestId };
}
