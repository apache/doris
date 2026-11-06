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

import { findProfileMatches } from './profileTextSearch';

describe('Profile text search', () => {
  it('returns original UTF-16 offsets after characters whose lowercase form expands', () => {
    expect(findProfileMatches('İ ExecTime İ EXECTIME', 'exectime')).toEqual([2, 13]);
    expect(findProfileMatches('İİ', 'İ')).toEqual([0, 1]);
  });

  it('matches Greek sigma regardless of its position in a word', () => {
    expect(findProfileMatches('ΟΣ οσ ος', 'οσ')).toEqual([0, 3, 6]);
  });

  it('keeps UTF-16 offsets when matching supplementary Unicode characters', () => {
    expect(findProfileMatches('😀𐐀 𐐨', '𐐨')).toEqual([2, 5]);
  });

  it.each(['.', '*', '+', '?', '^', '$', '{', '}', '(', ')', '|', '[', ']', '\\', '.*', '[a-z]+'])(
    'treats %j as literal text', (search) => {
      expect(findProfileMatches(`x${search} ${search}`, search)).toEqual([1, search.length + 2]);
    },
  );

  it('trims the search term and returns non-overlapping matches', () => {
    expect(findProfileMatches('aaaaa', ' aa ')).toEqual([0, 2]);
    expect(findProfileMatches('ExecTime EXECTIME', ' exectime ')).toEqual([0, 9]);
  });

  it.each(['', ' \t\n '])('does not search for an empty term: %j', (search) => {
    expect(findProfileMatches('ExecTime', search)).toEqual([]);
  });

  it('returns no matches when the term is absent or the profile is empty', () => {
    expect(findProfileMatches('ExecTime', 'missing')).toEqual([]);
    expect(findProfileMatches('', 'ExecTime')).toEqual([]);
  });
});
