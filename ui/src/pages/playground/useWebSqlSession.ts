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

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import {
  cancelWebSql,
  closeWebSqlSession,
  createWebSqlSession,
  executeWebSql,
  getWebSqlSession,
  resetWebSqlSession,
} from '../../api/webSql';
import { UiApiError } from '../../api/client';
import type { WebSqlExecutionResult } from '../../api/types';
import {
  claimSessionForTab,
  isClaimedByAnotherTab,
  storedSessionId,
  storeSessionId,
} from './sessionCoordinator';

export type WebSqlSessionStatus = 'connecting' | 'ready' | 'closed' | 'error';

const recoverableCodes = new Set([
  'WEB_SQL_SESSION_NOT_FOUND',
  'WEB_SQL_SESSION_EXPIRED',
  'WEB_SQL_ACCESS_DENIED',
]);

function isRecoverable(error: unknown): boolean {
  return error instanceof UiApiError && recoverableCodes.has(error.code);
}

export function useWebSqlSession() {
  const [status, setStatus] = useState<WebSqlSessionStatus>('connecting');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const sessionIdRef = useRef<string | null>(null);
  const initializationRef = useRef<Promise<string> | null>(null);
  const queueRef = useRef<Promise<unknown>>(Promise.resolve());
  const releaseClaimRef = useRef<() => void>(() => undefined);
  const mountedRef = useRef(true);

  const adoptSession = useCallback((id: string) => {
    releaseClaimRef.current();
    sessionIdRef.current = id;
    storeSessionId(id);
    releaseClaimRef.current = claimSessionForTab(id);
    if (mountedRef.current) {
      setSessionId(id);
      setError(null);
      setStatus('ready');
    }
    return id;
  }, []);

  const createSession = useCallback(async () => {
    if (initializationRef.current) return initializationRef.current;
    const pending = createWebSqlSession()
      .then((info) => adoptSession(info.sessionId))
      .finally(() => {
        initializationRef.current = null;
      });
    initializationRef.current = pending;
    return pending;
  }, [adoptSession]);

  useEffect(() => {
    mountedRef.current = true;
    const initialize = async () => {
      try {
        const stored = storedSessionId();
        if (stored && !(await isClaimedByAnotherTab(stored))) {
          try {
            const info = await getWebSqlSession(stored);
            adoptSession(info.sessionId);
          } catch (cause) {
            if (!isRecoverable(cause)) throw cause;
            storeSessionId(null);
            await createSession();
          }
        } else {
          if (stored) storeSessionId(null);
          await createSession();
        }
      } catch (cause) {
        if (mountedRef.current) {
          setError(cause);
          setStatus('error');
        }
      }
    };
    void initialize();
    return () => {
      mountedRef.current = false;
      releaseClaimRef.current();
      releaseClaimRef.current = () => undefined;
    };
  }, [adoptSession, createSession]);

  const ensureSession = useCallback(async () => {
    if (sessionIdRef.current) return sessionIdRef.current;
    return createSession();
  }, [createSession]);

  const replaceExpiredSession = useCallback(async (expiredId: string) => {
    if (sessionIdRef.current !== expiredId && sessionIdRef.current) return sessionIdRef.current;
    releaseClaimRef.current();
    releaseClaimRef.current = () => undefined;
    sessionIdRef.current = null;
    storeSessionId(null);
    if (mountedRef.current) {
      setSessionId(null);
      setStatus('connecting');
    }
    return createSession();
  }, [createSession]);

  const executeNow = useCallback(async (sql: string): Promise<WebSqlExecutionResult> => {
    const id = await ensureSession();
    try {
      return await executeWebSql(id, sql);
    } catch (cause) {
      if (!isRecoverable(cause)) throw cause;
      const replacement = await replaceExpiredSession(id);
      return executeWebSql(replacement, sql);
    }
  }, [ensureSession, replaceExpiredSession]);

  const execute = useCallback((sql: string): Promise<WebSqlExecutionResult> => {
    const pending = queueRef.current.then(() => executeNow(sql), () => executeNow(sql));
    queueRef.current = pending.then(() => undefined, () => undefined);
    return pending;
  }, [executeNow]);

  const cancel = useCallback(async () => {
    const id = await ensureSession();
    return cancelWebSql(id);
  }, [ensureSession]);

  const reset = useCallback(async () => {
    const task = async () => {
      const id = await ensureSession();
      try {
        const info = await resetWebSqlSession(id);
        adoptSession(info.sessionId);
        return info;
      } catch (cause) {
        if (!isRecoverable(cause)) throw cause;
        await replaceExpiredSession(id);
        return resetWebSqlSession(sessionIdRef.current!);
      }
    };
    const pending = queueRef.current.then(task, task);
    queueRef.current = pending.then(() => undefined, () => undefined);
    return pending;
  }, [adoptSession, ensureSession, replaceExpiredSession]);

  const close = useCallback(async () => {
    const task = async () => {
      const id = sessionIdRef.current;
      if (id) {
        try {
          await closeWebSqlSession(id);
        } catch (cause) {
          if (!isRecoverable(cause)) throw cause;
        }
      }
      releaseClaimRef.current();
      releaseClaimRef.current = () => undefined;
      sessionIdRef.current = null;
      storeSessionId(null);
      if (mountedRef.current) {
        setSessionId(null);
        setError(null);
        setStatus('closed');
      }
    };
    const pending = queueRef.current.then(task, task);
    queueRef.current = pending.then(() => undefined, () => undefined);
    return pending;
  }, []);

  const open = useCallback(async () => {
    setStatus('connecting');
    return createSession();
  }, [createSession]);

  return useMemo(
    () => ({ status, sessionId, error, execute, cancel, reset, close, open }),
    [cancel, close, error, execute, open, reset, sessionId, status],
  );
}
