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

import { executableSql, qualifiedName, quoteIdentifier } from './sqlSelection';

describe('Playground SQL selection', () => {
  it('executes the selected text when the selection is non-empty', () => {
    const document = 'SELECT 1;\nSELECT 2;';
    expect(executableSql(document, { from: 10, to: document.length })).toBe('SELECT 2;');
  });

  it('executes the full document for an empty selection and rejects whitespace', () => {
    expect(executableSql('  SELECT 1;  ', { from: 2, to: 2 })).toBe('SELECT 1;');
    expect(executableSql('   \n', { from: 0, to: 0 })).toBe('');
  });

  it('quotes every qualified identifier segment', () => {
    expect(quoteIdentifier('a`b')).toBe('`a``b`');
    expect(qualifiedName('internal', 'tpcds', 'store_sales')).toBe('`internal`.`tpcds`.`store_sales`');
  });
});
