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

import { buildClientTableView, compareCells, displayCell, type DynamicRow } from './dynamicTable';

const rows: DynamicRow[] = [
  { key: 'large', cells: ['9223372036854775807', 'alpha', null] },
  { key: 'small', cells: ['9', 'beta', ''] },
  { key: 'negative', cells: ['-12', 'alphabet', '\\N'] },
];

describe('dynamic table operations', () => {
  it('keeps large integer sorting precise without converting to Number', () => {
    expect(compareCells('9223372036854775807', '9007199254740993')).toBeGreaterThan(0);
    const view = buildClientTableView(rows, {
      search: '', sortColumn: 0, sortDirection: 'ascend', page: 1, pageSize: 10,
    });
    expect(view.rows.map((row) => row.key)).toEqual(['negative', 'small', 'large']);
  });

  it('combines filtering, sorting, pagination, and clamps an obsolete page', () => {
    const view = buildClientTableView(rows, {
      search: 'alpha', sortColumn: 1, sortDirection: 'descend', page: 8, pageSize: 1,
    });
    expect(view.total).toBe(2);
    expect(view.page).toBe(2);
    expect(view.rows).toHaveLength(1);
  });

  it('renders null, empty, and Doris null cells consistently', () => {
    expect(displayCell(null)).toBe('—');
    expect(displayCell('')).toBe('—');
    expect(displayCell('\\N')).toBe('—');
  });
});
