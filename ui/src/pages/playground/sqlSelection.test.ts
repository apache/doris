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

import { executableSql, qualifiedName, quoteIdentifier, sqlStatements, statementRangeAt, statementRanges } from './sqlSelection';

describe('Playground SQL selection', () => {
  it.each([
    '',
    ' \r\n\t',
    ';; ;',
    '-- comment;\r\n# another;\n/* block; */ ;',
    '/* first */ /* second */',
  ])('does not execute empty or comment-only text: %j', (document) => {
    expect(sqlStatements(document)).toEqual([]);
  });

  it('skips empty statements between queries and a trailing comment', () => {
    const document = '; /* before */ SELECT 1;; -- between;\n; SELECT 2; -- done;';
    expect(sqlStatements(document)).toEqual(['/* before */ SELECT 1;', 'SELECT 2;']);
  });

  it('preserves comments attached to SQL and comment markers in quoted text', () => {
    const document = "-- heading;\nSELECT ';', '--', '#', '/* */', `a;b`; /* done */";
    expect(sqlStatements(document)).toEqual([document.slice(0, document.indexOf('; /* done */') + 1)]);
  });

  it('keeps malformed SQL for server-side validation', () => {
    expect(sqlStatements('/* unterminated')).toEqual(['/* unterminated']);
    expect(sqlStatements("SELECT 'unterminated;")).toEqual(["SELECT 'unterminated;"]);
  });

  it('preserves editor ranges for empty statements and trailing comments', () => {
    const document = ';SELECT 1; -- done';
    expect(statementRanges(document)).toEqual([
      { from: 0, to: 1 },
      { from: 1, to: 10 },
      { from: 10, to: document.length },
    ]);
    expect(statementRangeAt(document, document.length)).toEqual({ from: 10, to: document.length });
  });

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
    expect(executableSql(document, { from: second, to: second })).toBe(document);
    expect(statementRangeAt(document, document.indexOf('c;d') + 2)).toEqual({ from: 0, to: document.indexOf('; --') + 1 });
    const secondRange = statementRangeAt(document, second);
    expect(document.slice(secondRange.from, secondRange.to)).toBe(' -- ignored;\nSELECT 2 /* ignored; */;');
  });

  it('does not split a hash comment at its semicolon', () => {
    const document = 'SELECT 1 # comment;\nFROM t;\nSELECT 3;';
    const cursor = document.indexOf('FROM');
    expect(executableSql(document, { from: cursor, to: cursor })).toBe(document);
    const range = statementRangeAt(document, cursor);
    expect(document.slice(range.from, range.to)).toBe('SELECT 1 # comment;\nFROM t;');
  });

  it('quotes every qualified identifier segment', () => {
    expect(quoteIdentifier('a`b')).toBe('`a``b`');
    expect(qualifiedName('internal', 'tpcds', 'store_sales')).toBe('`internal`.`tpcds`.`store_sales`');
  });
});
