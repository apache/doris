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
import { adaptCatalogs, adaptSchema, adaptSingleNameColumn } from './metadataAdapter';

function result(columnNames: string[], rows: unknown[][]): WebSqlExecutionResult {
  return {
    columns: columnNames.map((name) => ({ name, type: 'VARCHAR' })),
    rows,
    affectedRows: 0,
    elapsedTimeMs: 1,
    queryId: 'query-id',
    warnings: [],
    catalog: null,
    database: null,
    truncated: false,
  };
}

describe('Playground metadata adapters', () => {
  it('adapts catalog columns by name while preserving unknown server columns', () => {
    expect(adaptCatalogs(result(
      ['FutureField', 'IsCurrent', 'Type', 'CatalogName'],
      [['future', 'Yes', 'hms', 'warehouse']],
    ))).toEqual([{ name: 'warehouse', type: 'hms', current: true }]);
  });

  it('adapts database/table names and DESC output', () => {
    expect(adaptSingleNameColumn(result(['Database'], [['tpcds'], [null], ['demo']]))).toEqual(['tpcds', 'demo']);
    expect(adaptSchema(result(
      ['Field', 'Type', 'Null', 'Key', 'Default', 'Extra'],
      [['ss_item_sk', 'INT', 'No', 'true', null, 'NONE']],
    ))).toEqual([{
      name: 'ss_item_sk',
      type: 'INT',
      nullable: 'No',
      key: 'true',
      defaultValue: '',
      extra: 'NONE',
    }]);
  });
});
