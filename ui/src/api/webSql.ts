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

import { uiRequest } from './client';
import type {
  WebSqlCancelResult,
  WebSqlCloseResult,
  WebSqlExecutionResult,
  WebSqlSessionInfo,
} from './types';

const basePath = '/rest/v1/sql-sessions';

export async function createWebSqlSession(): Promise<WebSqlSessionInfo> {
  return uiRequest<WebSqlSessionInfo>(basePath, { method: 'POST' });
}

export async function getWebSqlSession(sessionId: string): Promise<WebSqlSessionInfo> {
  return uiRequest<WebSqlSessionInfo>(`${basePath}/${encodeURIComponent(sessionId)}`);
}

export async function executeWebSql(sessionId: string, sql: string): Promise<WebSqlExecutionResult> {
  return uiRequest<WebSqlExecutionResult>(`${basePath}/${encodeURIComponent(sessionId)}/statements`, {
      method: 'POST',
      body: JSON.stringify({ sql }),
    });
}

export async function cancelWebSql(sessionId: string): Promise<WebSqlCancelResult> {
  return uiRequest<WebSqlCancelResult>(`${basePath}/${encodeURIComponent(sessionId)}/cancel`, {
      method: 'POST',
    });
}

export async function resetWebSqlSession(sessionId: string): Promise<WebSqlSessionInfo> {
  return uiRequest<WebSqlSessionInfo>(`${basePath}/${encodeURIComponent(sessionId)}/reset`, {
      method: 'POST',
    });
}

export async function closeWebSqlSession(sessionId: string, keepalive = false): Promise<WebSqlCloseResult> {
  return uiRequest<WebSqlCloseResult>(`${basePath}/${encodeURIComponent(sessionId)}`, {
      method: 'DELETE',
      ...(keepalive ? { keepalive: true } : {}),
    });
}
