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

import { legacyGet, type LegacyTablePayload } from './operations';
import type { UiNodeTable, UiVersionInfo } from './types';

interface LegacyHardwareInfo {
  VersionInfo?: Record<string, unknown>;
}

function text(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(value);
}

function nodeTable(payload: LegacyTablePayload): UiNodeTable {
  const columnNames = Array.isArray(payload.column_names) ? payload.column_names.map(text) : [];
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  return {
    columnNames,
    rows: rows.map((row) => {
      if (Array.isArray(row)) return columnNames.map((_name, index) => text(row[index]));
      const record = row && typeof row === 'object' ? row as Record<string, unknown> : {};
      return columnNames.map((name) => text(record[name]));
    }),
  };
}

export async function fetchVersion(): Promise<UiVersionInfo> {
  const payload = await legacyGet<LegacyHardwareInfo>('/rest/v1/hardware_info/fe/version');
  const version = payload.VersionInfo ?? {};
  return {
    version: text(version.Version),
    git: text(version.Git),
    buildInfo: text(version.BuildInfo),
    buildTime: text(version.BuildTime),
    features: text(version.Features),
  };
}

export async function fetchFrontends(): Promise<UiNodeTable> {
  return nodeTable(await legacyGet<LegacyTablePayload>('/rest/v1/system?path=%2Ffrontends'));
}

export async function fetchBackends(): Promise<UiNodeTable> {
  return nodeTable(await legacyGet<LegacyTablePayload>('/rest/v1/system?path=%2Fbackends'));
}

export function useVersion() {
  return useQuery({ queryKey: ['ui', 'home', 'version'], queryFn: fetchVersion });
}

export function useFrontends(enabled: boolean) {
  return useQuery({
    queryKey: ['ui', 'nodes', 'frontends'],
    queryFn: fetchFrontends,
    enabled,
    refetchInterval: false,
  });
}

export function useBackends(enabled: boolean) {
  return useQuery({
    queryKey: ['ui', 'nodes', 'backends'],
    queryFn: fetchBackends,
    enabled,
    refetchInterval: false,
  });
}
