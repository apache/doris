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

export type DynamicCell = string | number | boolean | null | undefined;

export interface DynamicRow {
  key: string;
  cells: DynamicCell[];
  links?: Record<number, string>;
}

export interface DynamicTableData {
  columnNames: string[];
  rows: DynamicRow[];
}

export type SortDirection = 'ascend' | 'descend' | null;

export interface ClientTableQuery {
  search: string;
  sortColumn: number | null;
  sortDirection: SortDirection;
  page: number;
  pageSize: number;
}

export interface ClientTableView {
  rows: DynamicRow[];
  total: number;
  page: number;
}

const INTEGER_PATTERN = /^[+-]?\d+$/;

export function displayCell(value: DynamicCell): string {
  if (value === null || value === undefined || value === '' || value === '\\N') return '—';
  return String(value);
}

export function searchableCell(value: DynamicCell): string {
  if (value === null || value === undefined) return '';
  return String(value);
}

function compareIntegerStrings(left: string, right: string): number {
  const leftNegative = left.startsWith('-');
  const rightNegative = right.startsWith('-');
  if (leftNegative !== rightNegative) return leftNegative ? -1 : 1;

  const normalize = (value: string) => value.replace(/^[+-]?0+(?=\d)/, '').replace(/^\+/, '');
  const normalizedLeft = normalize(left).replace(/^-/, '');
  const normalizedRight = normalize(right).replace(/^-/, '');
  const magnitude = normalizedLeft.length === normalizedRight.length
    ? normalizedLeft.localeCompare(normalizedRight)
    : normalizedLeft.length - normalizedRight.length;
  return leftNegative ? -magnitude : magnitude;
}

export function compareCells(left: DynamicCell, right: DynamicCell): number {
  const leftText = searchableCell(left);
  const rightText = searchableCell(right);
  if (INTEGER_PATTERN.test(leftText) && INTEGER_PATTERN.test(rightText)) {
    return compareIntegerStrings(leftText, rightText);
  }
  return leftText.localeCompare(rightText, undefined, { numeric: true, sensitivity: 'base' });
}

export function filterRows(rows: DynamicRow[], search: string): DynamicRow[] {
  const normalized = search.trim().toLocaleLowerCase();
  if (!normalized) return rows;
  return rows.filter((row) => row.cells.some((cell) => searchableCell(cell).toLocaleLowerCase().includes(normalized)));
}

export function sortRows(rows: DynamicRow[], column: number | null, direction: SortDirection): DynamicRow[] {
  if (column === null || direction === null) return rows;
  const multiplier = direction === 'ascend' ? 1 : -1;
  return rows
    .map((row, position) => ({ row, position }))
    .sort((left, right) => {
      const compared = compareCells(left.row.cells[column], right.row.cells[column]);
      return compared === 0 ? left.position - right.position : compared * multiplier;
    })
    .map(({ row }) => row);
}

export function buildClientTableView(rows: DynamicRow[], query: ClientTableQuery): ClientTableView {
  const filtered = filterRows(rows, query.search);
  const sorted = sortRows(filtered, query.sortColumn, query.sortDirection);
  const pageSize = Math.max(1, query.pageSize);
  const lastPage = Math.max(1, Math.ceil(sorted.length / pageSize));
  const page = Math.min(Math.max(1, query.page), lastPage);
  const start = (page - 1) * pageSize;
  return { rows: sorted.slice(start, start + pageSize), total: sorted.length, page };
}
