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

suite("test_like_predicate_with_concat") {
    sql """ DROP TABLE IF EXISTS `test_like_predicate_with_concat` """

    sql """
        CREATE TABLE IF NOT EXISTS `test_like_predicate_with_concat` (
        `id` int,
        `value_col` string,
        `pattern_col` string
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ 
        INSERT INTO `test_like_predicate_with_concat` VALUES 
            (0, 'prefix0_infix0_suffix0', 'prefix0'),
            (1, 'prefix1_infix1_suffix1', 'prefix1'),
            (2, 'prefix2_infix2_suffix2', 'infix2'),
            (3, 'prefix3_infix3_suffix3', 'infix3'),
            (4, 'prefix4_infix4_suffix4', 'suffix4'),
            (5, 'prefix5_infix5_suffix5', 'suffix5'),
            (6, 'prefix6_infix6_suffix6', 'prefix6_infix6_suffix6'),
            (7, 'prefix7_infix7_suffix7', 'prefix7_infix7_suffix7'),
            (8, 'prefix8_infix8_suffix8', ''),
            (9, 'prefix9_infix9_suffix9', NULL);
        """

    // 1. test scalar case

    // 1.1 test STARTS_WITH case
    qt_sql1 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like 'prefix_%' ORDER BY `id`;
    """

    // 1.2 test ENDS_WITH case
    qt_sql2 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like '%suffix_' ORDER BY `id`;
    """

    // 2. test vector case with concat
    
    // 2.1 test ALLPASS case
    qt_sql3 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT('%', '%') ORDER BY `id`;
    """
    
    // 2.2 test EQUAL case
    qt_sql4 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT(pattern_col) ORDER BY `id`;
    """
    
    // 2.3 test SUBSTRING case
    qt_sql5 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT('%', pattern_col, '%') ORDER BY `id`;
    """
    
    // 2.4 test STARTS_WITH case
    qt_sql6 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT(pattern_col, '%') ORDER BY `id`;
    """

    // 2.5 test ENDS_WITH case
    qt_sql7 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT('%', pattern_col) ORDER BY `id`;
    """

    // 2.6 test REGEXP case
    qt_sql8 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` like CONCAT('prefix0_', 'infix0', '_suffix0') ORDER BY `id`;
    """
}
