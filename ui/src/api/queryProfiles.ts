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

import type { DynamicTableData } from '../components/operations/dynamicTable';
import { adaptLegacyTable, legacyGet, type LegacyTablePayload } from './operations';

export const queryProfilesKey = ['query-profiles'] as const;

export function filterQueryProfiles(data: DynamicTableData): DynamicTableData {
  const taskTypeColumn = data.columnNames.findIndex((name) => name.trim().toLocaleLowerCase() === 'task type');
  if (taskTypeColumn < 0) return data;
  return {
    ...data,
    rows: data.rows.filter((row) => String(row.cells[taskTypeColumn] ?? '').trim().toLocaleUpperCase() === 'QUERY'),
  };
}

export async function fetchQueryProfiles(): Promise<DynamicTableData> {
  return filterQueryProfiles(adaptLegacyTable(await legacyGet<LegacyTablePayload>('/rest/v1/query_profile')));
}

export async function fetchQueryProfileText(profileId: string): Promise<string> {
  const normalizedId = profileId.trim();
  if (!normalizedId) throw new Error('A Profile ID is required.');
  return legacyGet<string>(`/rest/v1/query_profile/text/${encodeURIComponent(normalizedId)}`);
}

export function useQueryProfiles(enabled = true) {
  return useQuery({
    queryKey: queryProfilesKey,
    queryFn: fetchQueryProfiles,
    enabled,
    refetchOnMount: 'always',
    refetchInterval: false,
  });
}

export function useQueryProfileText(profileId: string | undefined, enabled = true) {
  return useQuery({
    queryKey: [...queryProfilesKey, 'text', profileId ?? ''],
    queryFn: () => fetchQueryProfileText(profileId ?? ''),
    enabled: enabled && Boolean(profileId),
    refetchInterval: false,
  });
}
