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

import { cellValue, filterNodeRecords, summarizeNodes, toNodeRecords } from './nodeTable';

describe('dynamic node table data', () => {
  const table = {
    columnNames: ['Name', 'Alive', 'FutureMetric'],
    rows: [
      ['fe-1', 'true', 'a very long future value'],
      ['fe-2', 'false'],
      ['fe-3', 'unexpected', '42'],
    ],
  };

  it('preserves unknown columns, long values, and shorter rows', () => {
    const records = toNodeRecords(table);
    expect(cellValue(records[0], 2)).toBe('a very long future value');
    expect(cellValue(records[1], 2)).toBe('');
  });

  it('searches every returned field case-insensitively', () => {
    expect(filterNodeRecords(toNodeRecords(table), 'FUTURE VALUE')).toHaveLength(1);
    expect(filterNodeRecords(toNodeRecords(table), '42')[0]?.values[0]).toBe('fe-3');
  });

  it('summarizes true, false, and unrecognized Alive values without guessing', () => {
    expect(summarizeNodes(table)).toEqual({ total: 3, alive: 1, dead: 1, unknown: 1 });
    expect(summarizeNodes({ columnNames: ['Name'], rows: [['fe-1']] })).toEqual({
      total: 1,
      alive: 0,
      dead: 0,
      unknown: 1,
    });
  });
});
