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

suite("test_substring_index_columns") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_substring_index_compat"
    sql """
    CREATE TABLE test_substring_index_compat (
        no INT,
        sub_str VARCHAR(50),
        str VARCHAR(100)
    ) ENGINE=OLAP
    DUPLICATE KEY(no)
    DISTRIBUTED BY HASH(no) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """
    INSERT INTO test_substring_index_compat VALUES
        (1, 'BBB', 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06'),
        (2, 'ccc', 'zyz_01|zyz_02|CCC_03|qwe_04|qwe_05|qwe_06'),
        (3, 'DDD', 'AAA_01|BBB_02|CCC_03|DDD_04|EEE_05|FFF_06'),
        (4, 'DDD', 'sgr_01|wsc_02|CCC_03|DDD_04|rfv_05|rgb_06'),
        (5, 'eee', 'cdr_01|vfr_02|dfc_03|DDD_04|EEE_05|FFF_06'),
        (6, 'A_01', 'AAA_01|dsd_02|ert_03|bgt_04|fgh_05|hyb_06')
    """

    qt_sql """
    SELECT
        no,
        sub_str AS '分隔符字符串',
        str AS '需要截取的字符串',
        substring_index(str, sub_str, -1) AS '动态分隔符结果'
    FROM test_substring_index_compat
    ORDER BY no
    """

    sql """
    INSERT INTO test_substring_index_compat VALUES
        (7, '市', '北京市|上海市|广州市|深圳市'),
        (8, '人民', '中华人民共和国'),
        (9, '分隔符', '中文分隔符测试分隔符数据'),
        (10, '你好', '你好，世界！你好，朋友！')
    """

    qt_sql """
    SELECT
        no,
        sub_str AS '分隔符字符串',
        str AS '需要截取的字符串',
        substring_index(str, sub_str, 1) AS '正向截取',
        substring_index(str, sub_str, -1) AS '反向截取'
    FROM test_substring_index_compat
    WHERE no > 6
    ORDER BY no
    """

    sql "DROP TABLE IF EXISTS test_dynamic_params"
    sql """
    CREATE TABLE test_dynamic_params (
        id INT,
        source_str VARCHAR(100),
        delimiter VARCHAR(20),
        count_val INT
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """
    INSERT INTO test_dynamic_params VALUES
        (1, 'field1,field2,field3,field4', ',', 2),
        (2, 'field1,field2,field3,field4', ',', -1),
        (3, 'AAA_01|BBB_02|CCC_03', '|', 2),
        (4, 'AAA_01|BBB_02|CCC_03', '|', -2),
        (5, '中文分隔符测试分隔符数据', '分隔符', 1),
        (6, '中文分隔符测试分隔符数据', '分隔符', -1)
    """

    qt_sql """
    SELECT
        id,
        source_str,
        delimiter,
        count_val,
        substring_index(source_str, delimiter, count_val) AS result
    FROM test_dynamic_params
    ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_substring_index_compat"
    sql "DROP TABLE IF EXISTS test_dynamic_params"
}