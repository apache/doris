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

import type { WebSqlExecutionResult } from '../../api/types';

export interface CatalogItem {
  name: string;
  type: string;
  current: boolean;
}

export interface SchemaColumn {
  name: string;
  type: string;
  nullable: string;
  key: string;
  defaultValue: string;
  extra: string;
}

function columnIndex(result: WebSqlExecutionResult, candidates: string[], fallback: number): number {
  const normalized = result.columns.map((column) => column.name.toLowerCase().replaceAll(/[^a-z0-9]/g, ''));
  const found = candidates
    .map((candidate) => normalized.indexOf(candidate.toLowerCase().replaceAll(/[^a-z0-9]/g, '')))
    .find((index) => index !== -1);
  return found ?? fallback;
}

function cell(row: unknown[], index: number): string {
  if (index < 0 || index >= row.length || row[index] === null || row[index] === undefined) return '';
  const value = row[index];
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return `${value}`;
  if (typeof value === 'symbol') return value.description ?? '';
  return JSON.stringify(value) ?? '';
}

export function adaptCatalogs(result: WebSqlExecutionResult): CatalogItem[] {
  const name = columnIndex(result, ['CatalogName', 'Catalog'], result.columns.length > 1 ? 1 : 0);
  const type = columnIndex(result, ['Type'], 2);
  const current = columnIndex(result, ['IsCurrent', 'Current'], 3);
  return result.rows
    .map((row) => ({
      name: cell(row, name),
      type: cell(row, type),
      current: /^(true|yes)$/i.test(cell(row, current)),
    }))
    .filter((catalog) => catalog.name.length > 0);
}

export function adaptSingleNameColumn(result: WebSqlExecutionResult): string[] {
  return result.rows.map((row) => cell(row, 0)).filter(Boolean);
}

export function adaptSchema(result: WebSqlExecutionResult): SchemaColumn[] {
  const indexes = {
    name: columnIndex(result, ['Field', 'ColumnName'], 0),
    type: columnIndex(result, ['Type'], 1),
    nullable: columnIndex(result, ['Null', 'Nullable'], 2),
    key: columnIndex(result, ['Key'], 3),
    defaultValue: columnIndex(result, ['Default'], 4),
    extra: columnIndex(result, ['Extra'], 5),
  };
  return result.rows.map((row) => ({
    name: cell(row, indexes.name),
    type: cell(row, indexes.type),
    nullable: cell(row, indexes.nullable),
    key: cell(row, indexes.key),
    defaultValue: cell(row, indexes.defaultValue),
    extra: cell(row, indexes.extra),
  }));
}
