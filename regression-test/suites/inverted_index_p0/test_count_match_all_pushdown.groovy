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

suite("test_count_match_all_pushdown", "p0") {
    // Test that count(col MATCH_ALL 'xxx') should not push down storage layer aggregate.
    // MATCH_ALL generates a virtual column in OlapScan without originalColumn,
    // which previously caused NoSuchElementException on Optional.get().

    sql "DROP TABLE IF EXISTS test_count_match_all_pushdown"
    sql """
        CREATE TABLE test_count_match_all_pushdown (
            `id` INT NOT NULL,
            `content` TEXT NOT NULL,
            `nullable_content` TEXT NULL,
            INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            INDEX nullable_content_idx (`nullable_content`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO test_count_match_all_pushdown VALUES
        (1, 'hello world', 'foo bar'),
        (2, 'hello doris', 'foo baz'),
        (3, 'world doris', NULL);
    """

    sql "SET disable_nereids_rules='REWRITE_SIMPLE_AGG_TO_CONSTANT'"

    // count(MATCH_ALL expr) should not be pushed down to storage layer
    explain {
        sql("select count(content MATCH_ALL 'hello world') from test_count_match_all_pushdown;")
        contains "pushAggOp=NONE"
    }

    explain {
        sql("select count(content MATCH_ALL 'hello') from test_count_match_all_pushdown;")
        contains "pushAggOp=NONE"
    }

    // count(MATCH_ANY expr) should not be pushed down either
    explain {
        sql("select count(content MATCH_ANY 'hello world') from test_count_match_all_pushdown;")
        contains "pushAggOp=NONE"
    }

    // count(MATCH_PHRASE expr) should not be pushed down either
    explain {
        sql("select count(content MATCH_PHRASE 'hello world') from test_count_match_all_pushdown;")
        contains "pushAggOp=NONE"
    }

    // Verify actual execution returns correct results
    order_qt_match_all_count_1 "select count(content MATCH_ALL 'hello world') from test_count_match_all_pushdown;"
    order_qt_match_all_count_2 "select count(content MATCH_ALL 'hello') from test_count_match_all_pushdown;"
    order_qt_match_any_count "select count(content MATCH_ANY 'hello world') from test_count_match_all_pushdown;"
    order_qt_match_phrase_count "select count(content MATCH_PHRASE 'hello world') from test_count_match_all_pushdown;"

    // Test with complex WHERE clause (the original bug scenario)
    order_qt_match_all_with_where """
        select count(content MATCH_ALL 'hello') from test_count_match_all_pushdown
        where (case nullable_content when 'foo bar' then 1 end) = 1
           or not ((case id when 1 then 1 end) = 1 and (nullable_content is null));
    """
}
