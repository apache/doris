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

import type { UiNodeTable } from '../api/types';

export interface NodeRecord {
  key: string;
  values: string[];
}

export interface NodeSummary {
  total: number;
  alive: number;
  dead: number;
  unknown: number;
}

export function toNodeRecords(table: UiNodeTable): NodeRecord[] {
  return table.rows.map((values, index) => ({ key: String(index), values }));
}

export function cellValue(record: NodeRecord, columnIndex: number): string {
  return record.values[columnIndex] ?? '';
}

export function filterNodeRecords(records: NodeRecord[], search: string): NodeRecord[] {
  const needle = search.trim().toLocaleLowerCase();
  if (!needle) return records;
  return records.filter((record) => record.values.some((value) => value.toLocaleLowerCase().includes(needle)));
}

export function summarizeNodes(table: UiNodeTable): NodeSummary {
  const aliveIndex = table.columnNames.findIndex((name) => name.toLocaleLowerCase() === 'alive');
  if (aliveIndex < 0) return { total: table.rows.length, alive: 0, dead: 0, unknown: table.rows.length };

  return table.rows.reduce<NodeSummary>(
    (summary, row) => {
      const value = (row[aliveIndex] ?? '').trim().toLocaleLowerCase();
      if (value === 'true') summary.alive += 1;
      else if (value === 'false') summary.dead += 1;
      else summary.unknown += 1;
      return summary;
    },
    { total: table.rows.length, alive: 0, dead: 0, unknown: 0 },
  );
}
