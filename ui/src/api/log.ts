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

import { UiApiError } from './client';
import { legacyGet, legacyPostForm } from './operations';
import type { UiLogSnapshot } from './types';

export const logQueryKey = ['ui', 'log'] as const;

interface LegacyLogPayload {
  LogConfiguration?: Record<string, unknown>;
  LogContents?: Record<string, unknown>;
}

function text(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(value);
}

function names(value: unknown): string[] {
  return text(value).split(',').map((name) => name.trim()).filter(Boolean);
}

function logText(value: unknown): string {
  return text(value).replace(/^<pre>/, '').replace(/<\/pre>$/, '').replace(/<\/br>/g, '\n');
}

export async function fetchLog(): Promise<UiLogSnapshot> {
  const payload = await legacyGet<LegacyLogPayload>('/rest/v1/log');
  const configuration = payload.LogConfiguration ?? {};
  const contents = payload.LogContents ?? {};
  const showingLast = text(contents.showingLast);
  return {
    level: text(configuration.Level),
    mode: text(configuration.Mode),
    verboseNames: names(configuration.VerboseNames),
    auditNames: names(configuration.AuditNames),
    logPath: text(contents.logPath),
    showingLastBytes: Number(showingLast.match(/\d+/)?.[0] ?? 0),
    contents: contents.plainLog === undefined ? logText(contents.log) : text(contents.plainLog),
    contentError: contents.error ? text(contents.error) : null,
  };
}

export async function addVerboseName(name: string): Promise<void> {
  await updateVerboseName('add_verbose', name);
}

export async function deleteVerboseName(name: string): Promise<void> {
  await updateVerboseName('del_verbose', name);
}

async function updateVerboseName(parameter: 'add_verbose' | 'del_verbose', rawName: string): Promise<void> {
  const name = rawName.trim();
  if (!/^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)*$/.test(name) || name.length > 256) {
    throw new UiApiError(400, {
      code: 'UI_LOG_VERBOSE_INVALID',
      message: 'Enter a valid Java package or logger name (maximum 256 characters).',
      requestId: 'client',
    });
  }
  await legacyPostForm<Record<string, unknown>>('/rest/v1/log', new URLSearchParams({ [parameter]: name }));
}

export function useLog(enabled: boolean) {
  return useQuery({ queryKey: logQueryKey, queryFn: fetchLog, enabled, refetchInterval: false });
}
