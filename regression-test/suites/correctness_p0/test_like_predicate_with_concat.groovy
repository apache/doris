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
            (1, '%prefix1_infix1_suffix1', 'prefix1'),
            (2, 'prefix2_\$infix2\$_suffix2', 'infix2'),
            (3, 'prefix3_^infix3_suffix3', 'infix3'),
            (4, '\$prefix4_\$infix4%%%_^^suffix4', 'suffix4'),
            (5, 'prefix5%%\$__infix5\$_^^^%%\$suffix5', 'suffix5'),
            (6, 'prefix6__^^_%%%__infix6_%^suffix6%', 'prefix6__^^_%%%__infix6_%^suffix6%'),
            (7, '%%%^^^\$prefix7_infix7_suffix7%%%^^^\$', 'prefix7_infix7_suffix7'),
            (8, 'prefix8_^^%%%infix8%%\$^^___suffix8', ''),
            (9, 'prefix9\$%%%^^__infix9__&&%%\$suffix9', NULL);
        """

    qt_sql1 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE 'prefix_%' ORDER BY `id`;
    """

    qt_sql2 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE '%suffix_' ORDER BY `id`;
    """

    qt_sql3 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT('%', '%') ORDER BY `id`;
    """
    
    qt_sql4 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT(pattern_col) ORDER BY `id`;
    """
    
    qt_sql5 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT('%', pattern_col, '%') ORDER BY `id`;
    """
    
    qt_sql6 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT(pattern_col, '%') ORDER BY `id`;
    """

    qt_sql7 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT('%', pattern_col) ORDER BY `id`;
    """

    qt_sql8 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` LIKE CONCAT('prefix0_', 'infix0', '_suffix0') ORDER BY `id`;
    """

    qt_sql9 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP '^prefix' ORDER BY `id`;
    """

    qt_sql10 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP '.*suffix.\$' ORDER BY `id`;
    """

    // TODO: fix bug in master branch
    // qt_sql11 """
    //     SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP '.*' ORDER BY `id`;
    // """

    // TODO: fix bug in master branch
    // qt_sql12 """
    //     SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP CONCAT('.', '*') ORDER BY `id`;
    // """
    
    qt_sql13 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP CONCAT('.*', pattern_col, '.*') ORDER BY `id`;
    """
    
    qt_sql14 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP CONCAT('^', pattern_col, '.*') ORDER BY `id`;
    """

    qt_sql15 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP CONCAT(pattern_col, '\$') ORDER BY `id`;
    """

    qt_sql16 """
        SELECT * FROM `test_like_predicate_with_concat` WHERE `value_col` REGEXP CONCAT('prefix0_', 'infix0', '_suffix0') ORDER BY `id`;
    """
}
