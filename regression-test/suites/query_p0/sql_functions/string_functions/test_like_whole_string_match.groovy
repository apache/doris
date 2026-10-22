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

suite("test_like_whole_string_match") {
    sql "drop table if exists test_like_whole_string_match"
    sql """
        create table test_like_whole_string_match (
            id int,
            s varchar(64)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        insert into test_like_whole_string_match values
            (1, 'acb'),
            (2, concat('acb', char(10))),
            (3, concat('acb', char(13), char(10))),
            (4, concat('a', char(10), 'b')),
            (5, 'acbx'),
            (6, 'ab*'),
            (7, concat('ab*', char(10))),
            (8, 'ab*xyz');
    """

    // a value that ends with a newline is one character longer, so it must not match a
    // pattern that is anchored at the tail
    qt_anchor_underscore """
        select id, length(s) as len, s like 'a_b' as r from test_like_whole_string_match order by id
    """
    qt_anchor_leading_percent """
        select id, length(s) as len, s like '%c%b' as r from test_like_whole_string_match order by id
    """
    // a literal '*' at the end of the pattern does not make it end with a wildcard
    qt_literal_star_tail """
        select id, length(s) as len, s like 'a_*' as r from test_like_whole_string_match order by id
    """
    // a pattern that really ends with '%' still accepts anything, the newline included
    qt_trailing_percent """
        select id, length(s) as len, s like 'a_b%' as r from test_like_whole_string_match order by id
    """

    // a pattern without wildcards is rewritten to an equality by LIKE_TO_EQUAL, which would
    // make this block pass with LIKE broken; disable the rule so the shortcut path really runs
    sql "set disable_nereids_expression_rules='LIKE_TO_EQUAL'"
    qt_equals_shortcut """
        select id, length(s) as len, s like 'acb' as r from test_like_whole_string_match order by id
    """

    // a LIKE in a filter with function pushdown enabled is evaluated by LikeColumnPredicate,
    // which converts the pattern a second time through LikeSearchState::clone()
    sql "set enable_function_pushdown = true"
    qt_pushdown_underscore """
        select id from test_like_whole_string_match where s like 'a_b' order by id
    """
    qt_pushdown_literal_star """
        select id from test_like_whole_string_match where s like 'a_*' order by id
    """
    // NOT LIKE reaches the scan as CompoundPredicate(NOT, LIKE), whose children are not a slot,
    // so it stays a generic expression rather than a LikeColumnPredicate
    qt_not_literal_star """
        select id from test_like_whole_string_match where s not like 'a_*' order by id
    """
    qt_pushdown_trailing_percent """
        select id from test_like_whole_string_match where s like 'a_b%' order by id
    """
}
