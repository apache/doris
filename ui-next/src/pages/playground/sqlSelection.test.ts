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

import { executableSql, qualifiedName, quoteIdentifier, statementRangeAt } from './sqlSelection';

describe('Playground SQL selection', () => {
  it('executes the selected text when the selection is non-empty', () => {
    const document = 'SELECT 1;\nSELECT 2;';
    expect(executableSql(document, { from: 10, to: document.length })).toBe('SELECT 2;');
  });

  it('executes the whole document when the selection is empty and rejects whitespace', () => {
    const document = 'SELECT 1;\nSELECT 2;';
    expect(executableSql(document, { from: document.indexOf('SELECT 2'), to: document.indexOf('SELECT 2') })).toBe(document);
    expect(executableSql('   \n', { from: 0, to: 0 })).toBe('');
  });

  it('ignores semicolons inside strings and comments', () => {
    const document = "SELECT 'a;b', \"c;d\", `e;f`; -- ignored;\nSELECT 2 /* ignored; */;";
    const second = document.lastIndexOf('SELECT 2');
    expect(executableSql(document, { from: second, to: second })).toBe('SELECT 2 /* ignored; */;');
    expect(statementRangeAt(document, document.indexOf('c;d') + 2)).toEqual({ from: 0, to: document.indexOf('; --') + 1 });
  });

  it('does not split a hash comment at its semicolon', () => {
    const document = 'SELECT 1 # comment;\nFROM t;\nSELECT 3;';
    expect(executableSql(document, { from: document.indexOf('FROM'), to: document.indexOf('FROM') })).toBe('SELECT 1 # comment;\nFROM t;');
  });

  it('quotes every qualified identifier segment', () => {
    expect(quoteIdentifier('a`b')).toBe('`a``b`');
    expect(qualifiedName('internal', 'tpcds', 'store_sales')).toBe('`internal`.`tpcds`.`store_sales`');
  });
});
