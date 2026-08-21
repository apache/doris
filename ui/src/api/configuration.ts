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

import { legacyPostJsonMutation } from './operations';

export type ConfigurationScope = 'fe' | 'be';

export interface ConfigurationRow {
  key: string;
  name: string;
  node: string;
  nodeType: string;
  valueType: string;
  masterOnly: boolean | null;
  currentValue: string;
  mutable: boolean;
}

interface ConfigurationPayload {
  column_names?: unknown;
  columnNames?: unknown;
  rows?: unknown;
}

export interface ConfigurationUpdate {
  scope: ConfigurationScope;
  name: string;
  nodes: string[];
  value: string;
  persist: boolean;
}

export interface ConfigurationUpdateFailure {
  configName: string;
  value: string;
  node: string;
  error: string;
}

export interface ConfigurationUpdateResult {
  failures: ConfigurationUpdateFailure[];
}

interface ConfigurationUpdatePayload {
  failed?: unknown;
}

const aliases = {
  name: ['配置项', 'Name'],
  node: ['节点', 'Node'],
  nodeType: ['节点类型', 'Node Type'],
  valueType: ['配置值类型', 'Value Type'],
  masterOnly: ['MasterOnly', 'Master Only'],
  currentValue: ['配置值', 'Current Value', 'Value'],
  mutable: ['可修改', 'Mutable'],
} as const;

function columnIndex(columns: string[], names: readonly string[], fallback: number): number {
  const index = names.map((name) => columns.indexOf(name)).find((candidate) => candidate !== -1);
  return index ?? fallback;
}

function cell(row: unknown[], index: number): string {
  if (index < 0 || index >= row.length) return '';
  const value = row[index];
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  return JSON.stringify(value);
}

function booleanValue(value: string): boolean {
  return /^(true|yes|1)$/i.test(value.trim());
}

export function adaptConfiguration(payload: ConfigurationPayload, scope: ConfigurationScope): ConfigurationRow[] {
  const rawColumns = payload.column_names ?? payload.columnNames;
  const columns = Array.isArray(rawColumns) ? rawColumns.map(String) : [];
  const fallback = scope === 'fe'
    ? { name: 0, node: 1, nodeType: 2, valueType: 3, masterOnly: 4, currentValue: 5, mutable: 6 }
    : { name: 0, node: 1, nodeType: 2, valueType: 3, masterOnly: -1, currentValue: 4, mutable: 5 };
  const indexes = {
    name: columnIndex(columns, aliases.name, fallback.name),
    node: columnIndex(columns, aliases.node, fallback.node),
    nodeType: columnIndex(columns, aliases.nodeType, fallback.nodeType),
    valueType: columnIndex(columns, aliases.valueType, fallback.valueType),
    masterOnly: columnIndex(columns, aliases.masterOnly, fallback.masterOnly),
    currentValue: columnIndex(columns, aliases.currentValue, fallback.currentValue),
    mutable: columnIndex(columns, aliases.mutable, fallback.mutable),
  };
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  return rows.filter(Array.isArray).map((rawRow, rowIndex) => {
    const row = rawRow as unknown[];
    const name = cell(row, indexes.name);
    const node = cell(row, indexes.node);
    return {
      key: `${scope}:${node}:${name}:${rowIndex}`,
      name,
      node,
      nodeType: cell(row, indexes.nodeType) || scope.toUpperCase(),
      valueType: cell(row, indexes.valueType),
      masterOnly: indexes.masterOnly < 0 ? null : booleanValue(cell(row, indexes.masterOnly)),
      currentValue: cell(row, indexes.currentValue),
      mutable: booleanValue(cell(row, indexes.mutable)),
    };
  });
}

export async function fetchConfiguration(scope: ConfigurationScope): Promise<ConfigurationRow[]> {
  const payload = await legacyPostJsonMutation<ConfigurationPayload>(
    `/rest/v2/manager/node/configuration_info?type=${scope}`,
    {},
  );
  return adaptConfiguration(payload, scope);
}

function updateFailure(value: unknown): ConfigurationUpdateFailure | null {
  if (!value || typeof value !== 'object') return null;
  const failure = value as Record<string, unknown>;
  return {
    configName: cell([failure.config_name], 0),
    value: cell([failure.value], 0),
    node: cell([failure.node], 0),
    error: cell([failure.err_info], 0) || 'The node rejected the configuration change.',
  };
}

export async function updateConfiguration(update: ConfigurationUpdate): Promise<ConfigurationUpdateResult> {
  const payload = await legacyPostJsonMutation<ConfigurationUpdatePayload>(
    `/rest/v2/manager/node/set_config/${update.scope}`,
    {
      [update.name]: {
        node: update.nodes,
        value: update.value,
        persist: update.persist,
      },
    },
  );
  const failures = Array.isArray(payload.failed)
    ? payload.failed.map(updateFailure).filter((failure): failure is ConfigurationUpdateFailure => failure !== null)
    : [];
  return { failures };
}

export function useConfiguration(scope: ConfigurationScope) {
  return useQuery({
    queryKey: ['configuration', scope],
    queryFn: () => fetchConfiguration(scope),
    refetchInterval: false,
  });
}
