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

suite("count_constant_rewrite") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS count_constant_rewrite_test"
    sql """
        CREATE TABLE count_constant_rewrite_test (
            id INT,
            js VARCHAR(100)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1');
    """

    sql """
        INSERT INTO count_constant_rewrite_test VALUES
            (1, '{"a": 1}'),
            (2, '{"a": 2}'),
            (3, '{"b": 3}'),
            (4, NULL);
    """
    sql "sync"

    // Non-null row-independent json_extract should be equivalent to count(*).
    qt_count_constant_json_extract_non_null '''
        SELECT count(json_extract('{"a": 1}', '$.a')) FROM count_constant_rewrite_test
    '''

    // Null row-independent json_extract should still return zero.
    qt_count_constant_json_extract_null '''
        SELECT count(json_extract('{"a": 1}', '$.missing')) FROM count_constant_rewrite_test
    '''

    // Cover the multi-path JSON expression shape that motivated this rewrite.
    qt_count_constant_json_extract_multi_path '''
        SELECT count(json_extract(
            '{"sparam":{"nested_1":"test_string","nested_2":"test_2"}, "nparam":8495, "fparam":{"nested_1":91.15,"nested_2":[334, 89.05, 1000.01]}, "bparam":false}',
            '$.fparam',
            '$.fparam.nested_2',
            '$[last-2]'
        )) FROM count_constant_rewrite_test
    '''

    // Group-by keeps per-group row counts while evaluating the constant expression above aggregation.
    order_qt_count_constant_json_extract_group_by '''
        SELECT id % 2, count(json_extract('{"a": 1}', '$.a'))
        FROM count_constant_rewrite_test
        GROUP BY id % 2
        ORDER BY id % 2
    '''

    // Column-dependent json_extract must not be rewritten as count(*).
    qt_count_non_constant_json_extract '''
        SELECT count(json_extract(cast(js AS JSON), '$.a')) FROM count_constant_rewrite_test
    '''

    // Keep count(*) visible in explain, instead of folding it to a metadata constant.
    sql "SET disable_nereids_rules='REWRITE_SIMPLE_AGG_TO_CONSTANT'"

    explain {
        sql('''
            SELECT count(json_extract('{"a": 1}', '$.a')) FROM count_constant_rewrite_test
        ''')
        contains "pushAggOp=COUNT"
    }

    explain {
        sql('''
            SELECT count(json_extract('{"a": 1}', '$.missing')) FROM count_constant_rewrite_test
        ''')
        contains "pushAggOp=COUNT"
    }

    explain {
        sql('''
            SELECT count(json_extract(cast(js AS JSON), '$.a')) FROM count_constant_rewrite_test
        ''')
        contains "pushAggOp=NONE"
    }
}
