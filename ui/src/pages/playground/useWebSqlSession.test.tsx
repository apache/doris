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

import { renderHook, waitFor } from '@testing-library/react';

import { UiApiError } from '../../api/client';
import {
  closeWebSqlSession,
  createWebSqlSession,
  getWebSqlSession,
} from '../../api/webSql';
import { WEB_SQL_SESSION_STORAGE_KEY } from './sessionCoordinator';
import { useWebSqlSession } from './useWebSqlSession';

vi.mock('../../api/webSql', () => ({
  cancelWebSql: vi.fn(),
  closeWebSqlSession: vi.fn(),
  createWebSqlSession: vi.fn(),
  executeWebSql: vi.fn(),
  getWebSqlSession: vi.fn(),
  resetWebSqlSession: vi.fn(),
}));

const storedId = 'storedFE.a1234567890123456789012345678901234567890';
const replacementId = 'newFront.a1234567890123456789012345678901234567890';

describe('useWebSqlSession initialization', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    sessionStorage.clear();
    vi.mocked(closeWebSqlSession).mockResolvedValue({ closed: true });
  });

  it('validates a stored session before reporting ready', async () => {
    sessionStorage.setItem(WEB_SQL_SESSION_STORAGE_KEY, storedId);
    vi.mocked(getWebSqlSession).mockResolvedValue({
      sessionId: storedId,
      createdAtMillis: 1,
      lastAccessMillis: 2,
    });

    const { result, unmount } = renderHook(() => useWebSqlSession());

    expect(result.current.status).toBe('connecting');
    await waitFor(() => expect(result.current.status).toBe('ready'));
    expect(result.current.sessionId).toBe(storedId);
    expect(getWebSqlSession).toHaveBeenCalledWith(storedId);
    expect(createWebSqlSession).not.toHaveBeenCalled();
    unmount();
    expect(closeWebSqlSession).not.toHaveBeenCalled();
    expect(sessionStorage.getItem(WEB_SQL_SESSION_STORAGE_KEY)).toBe(storedId);
  });

  it('replaces a stored session that no longer exists in the FE', async () => {
    sessionStorage.setItem(WEB_SQL_SESSION_STORAGE_KEY, storedId);
    vi.mocked(getWebSqlSession).mockRejectedValue(new UiApiError(404, {
      code: 'WEB_SQL_SESSION_NOT_FOUND',
      message: 'The SQL session does not exist.',
      requestId: 'request-1',
    }));
    vi.mocked(createWebSqlSession).mockResolvedValue({
      sessionId: replacementId,
      createdAtMillis: 3,
      lastAccessMillis: 3,
    });

    const { result, unmount } = renderHook(() => useWebSqlSession());

    await waitFor(() => expect(result.current.status).toBe('ready'));
    expect(result.current.sessionId).toBe(replacementId);
    expect(sessionStorage.getItem(WEB_SQL_SESSION_STORAGE_KEY)).toBe(replacementId);
    expect(createWebSqlSession).toHaveBeenCalledTimes(1);
    unmount();
  });

  it('keeps the session available for reload and relies on idle cleanup after a discarded page', async () => {
    vi.mocked(createWebSqlSession).mockResolvedValue({
      sessionId: replacementId,
      createdAtMillis: 3,
      lastAccessMillis: 3,
    });
    const { result, unmount } = renderHook(() => useWebSqlSession());

    await waitFor(() => expect(result.current.status).toBe('ready'));
    window.dispatchEvent(new Event('pagehide'));

    expect(closeWebSqlSession).not.toHaveBeenCalled();
    unmount();
    expect(closeWebSqlSession).not.toHaveBeenCalled();
    expect(sessionStorage.getItem(WEB_SQL_SESSION_STORAGE_KEY)).toBe(replacementId);
  });
});
