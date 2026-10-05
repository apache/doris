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

export interface SqlSelection {
  from: number;
  to: number;
}

export interface SqlRange {
  from: number;
  to: number;
}

/** Returns the semicolon-delimited statement containing offset, ignoring quoted text and comments. */
export function statementRanges(document: string): SqlRange[] {
  const ranges: SqlRange[] = [];
  let start = 0;
  let quote: "'" | '"' | '`' | null = null;
  let lineComment = false;
  let blockComment = false;

  for (let index = 0; index < document.length; index += 1) {
    const character = document[index];
    const next = document[index + 1];

    if (lineComment) {
      if (character === '\n' || character === '\r') lineComment = false;
      continue;
    }
    if (blockComment) {
      if (character === '*' && next === '/') {
        blockComment = false;
        index += 1;
      }
      continue;
    }
    if (quote) {
      if (character === '\\') {
        index += 1;
      } else if (character === quote) {
        // SQL permits doubled quote characters inside quoted literals/identifiers.
        if (next === quote) index += 1;
        else quote = null;
      }
      continue;
    }

    if (character === "'" || character === '"' || character === '`') {
      quote = character;
    } else if (character === '-' && next === '-') {
      lineComment = true;
      index += 1;
    } else if (character === '#') {
      lineComment = true;
    } else if (character === '/' && next === '*') {
      blockComment = true;
      index += 1;
    } else if (character === ';') {
      const end = index + 1;
      ranges.push({ from: start, to: end });
      start = end;
    }
  }
  if (start < document.length) ranges.push({ from: start, to: document.length });
  return ranges;
}

export function statementRangeAt(document: string, offset: number): SqlRange {
  const cursor = Math.max(0, Math.min(offset, document.length));
  const ranges = statementRanges(document);
  return ranges.find((range) => cursor <= range.to) ?? { from: 0, to: document.length };
}

export function sqlStatements(document: string): string[] {
  return statementRanges(document)
    .map(({ from, to }) => document.slice(from, to).trim())
    .filter(Boolean);
}

export function executableSql(document: string, selection: SqlSelection): string {
  // A selection is an explicit request to run only that text. With no
  // selection, run the complete editor document so multi-statement scripts
  // continue to work from the Run button.
  const range = selection.from === selection.to ? { from: 0, to: document.length } : selection;
  const selected = document.slice(range.from, range.to);
  return selected.trim();
}

export function quoteIdentifier(identifier: string): string {
  return `\`${identifier.replaceAll('`', '``')}\``;
}

export function qualifiedName(...identifiers: string[]): string {
  return identifiers.map(quoteIdentifier).join('.');
}
