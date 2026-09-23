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

suite("test_regexp_repeated_search_anchor") {
    qt_anchor_functions """
        SELECT regexp_count('aaa', '^a'),
               regexp_extract_all('aaa', '(^a)'),
               regexp_extract_all_array('aaa', '(^a)'),
               split_by_regexp('aaa', '^a'),
               size(split_by_regexp('aaa', '^a'))
    """

    qt_word_boundary_functions """
        SELECT regexp_count(repeat('a', 10000), '\\\\b'),
               regexp_extract_all(repeat('a', 10000), '(\\\\b)'),
               regexp_extract_all_array(repeat('a', 10000), '(\\\\b)')
    """
}
