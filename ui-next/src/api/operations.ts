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

import { useQuery } from '@tanstack/react-query';

import type { DynamicCell, DynamicTableData } from '../components/operations/dynamicTable';
import { UiApiError } from './client';
import { getCsrfToken, setCsrfToken } from './csrf';

export interface LegacyEnvelope<T> {
  code?: number;
  msg?: string;
  data?: T;
}

export interface LegacyTablePayload {
  column_names?: unknown;
  rows?: unknown;
  href_columns?: unknown;
  href_column?: unknown;
  parent_url?: unknown;
}

export interface SystemResult {
  table: DynamicTableData;
  parentPath: string | null;
}

function asCell(value: unknown): DynamicCell {
  if (value === null || value === undefined || typeof value === 'string'
    || typeof value === 'number' || typeof value === 'boolean') return value;
  return JSON.stringify(value);
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => String(item)) : [];
}

export async function legacyGet<T>(path: string): Promise<T> {
  return legacyRequest<T>(path, { method: 'GET' });
}

export async function legacyPostForm<T>(path: string, form: URLSearchParams): Promise<T> {
  return legacyRequest<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body: form.toString(),
  });
}

export async function legacyPostJson<T>(path: string, body: unknown): Promise<T> {
  return legacyRequest<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

export async function legacyPostJsonMutation<T>(path: string, body: unknown): Promise<T> {
  const token = getCsrfToken();
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['X-Doris-CSRF-Token'] = token;
  return legacyRequest<T>(path, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
}

async function legacyRequest<T>(path: string, init: RequestInit): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Accept', 'application/json');
  const response = await fetch(path, {
    ...init,
    headers,
    credentials: 'same-origin',
  });
  const requestId = response.headers.get('X-Request-Id') ?? 'unknown';
  let payload: LegacyEnvelope<T> | null = null;
  try {
    payload = await response.json() as LegacyEnvelope<T>;
  } catch {
    throw new UiApiError(response.status, {
      code: 'UI_INVALID_RESPONSE',
      message: 'The server returned invalid JSON.',
      requestId,
    });
  }

  if (response.status === 401) {
    setCsrfToken(null);
    window.dispatchEvent(new CustomEvent('doris-ui:unauthorized'));
  }
  if (!response.ok || payload.code !== 0 || payload.data === undefined) {
    throw new UiApiError(response.status || 500, {
      code: response.status === 401 ? 'UI_UNAUTHENTICATED'
        : response.status === 403 ? 'UI_FORBIDDEN'
          : 'UI_OPERATION_FAILED',
      message: payload.msg || 'The operational data could not be loaded.',
      requestId,
    });
  }
  return payload.data;
}

export function adaptLegacyTable(payload: LegacyTablePayload): DynamicTableData {
  const columnNames = asStringArray(payload.column_names);
  const hrefColumns = asStringArray(payload.href_columns ?? payload.href_column);
  const rawRows = Array.isArray(payload.rows) ? payload.rows : [];
  return {
    columnNames,
    rows: rawRows.map((rawRow, rowIndex) => {
      const record = rawRow && typeof rawRow === 'object' && !Array.isArray(rawRow)
        ? rawRow as Record<string, unknown>
        : null;
      const cells = Array.isArray(rawRow)
        ? columnNames.map((_name, columnIndex) => asCell(rawRow[columnIndex]))
        : columnNames.map((name) => asCell(record?.[name]));
      const hrefPaths = asStringArray(record?.__hrefPaths);
      const links = hrefColumns.reduce<Record<number, string>>((result, name, hrefIndex) => {
        const columnIndex = columnNames.indexOf(name);
        const href = hrefPaths[hrefIndex];
        if (columnIndex >= 0 && href) result[columnIndex] = href;
        return result;
      }, {});
      return { key: `row-${rowIndex}`, cells, ...(Object.keys(links).length > 0 ? { links } : {}) };
    }),
  };
}

export function normalizeProcPath(path: string): string {
  const decoded = path.trim();
  if (!decoded || decoded === '/') return '/';
  return `/${decoded.split('/').filter(Boolean).join('/')}`;
}

export function procPathFromHref(href: string | null | undefined): string | null {
  if (!href) return null;
  try {
    const url = new URL(href, 'http://doris.local');
    if (url.pathname !== '/rest/v1/system') return null;
    return normalizeProcPath(url.searchParams.get('path') ?? '/');
  } catch {
    return null;
  }
}

export async function fetchSystem(path: string): Promise<SystemResult> {
  const normalized = normalizeProcPath(path);
  const search = new URLSearchParams({ path: normalized });
  const payload = await legacyGet<LegacyTablePayload>(`/rest/v1/system?${search.toString()}`);
  return {
    table: adaptLegacyTable(payload),
    parentPath: procPathFromHref(typeof payload.parent_url === 'string' ? payload.parent_url : null),
  };
}

export async function fetchSessions(): Promise<DynamicTableData> {
  return adaptLegacyTable(await legacyGet<LegacyTablePayload>('/rest/v1/session'));
}

export function useSystem(path: string, enabled: boolean) {
  return useQuery({
    queryKey: ['operations', 'system', normalizeProcPath(path)],
    queryFn: () => fetchSystem(path),
    enabled,
    refetchInterval: false,
  });
}

export function useSessions(enabled: boolean) {
  return useQuery({
    queryKey: ['operations', 'sessions'],
    queryFn: fetchSessions,
    enabled,
    refetchOnMount: 'always',
    refetchInterval: false,
  });
}
