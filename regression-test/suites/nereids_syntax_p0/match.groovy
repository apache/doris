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

import java.util.stream.Collectors

suite("match") {
    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """
        drop table if exists test_nereids_match_select;
    """

    sql """
        CREATE TABLE `test_nereids_match_select`
        (
            `name`           varchar(50),
            `age`            int NOT NULL,
            `grade`          varchar(30) NOT NULL,
            `fatherName`     varchar(50),
            `matherName`     varchar(50),
            `selfComment`    text,
            INDEX name_idx(name) USING INVERTED PROPERTIES("parser"="english") COMMENT 'name index',
            INDEX age_idx(age) USING INVERTED COMMENT 'age index',
            INDEX grade_idx(grade) USING INVERTED PROPERTIES("parser"="none") COMMENT 'grade index',
            INDEX fatherName_idx(fatherName) USING INVERTED PROPERTIES("parser"="english") COMMENT 'fatherName index',
            INDEX matherName_idx(matherName) USING INVERTED PROPERTIES("parser"="english") COMMENT 'matherName index',
            INDEX selfComment_idx(selfComment) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'selfComment index'
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """
    
    sql """ insert into test_nereids_match_select VALUES
        ("zhang san", 10, "grade 5", "zhang yi", "chen san", "Class activists"),
        ("zhang san yi", 11, "grade 5", "zhang yi", "chen san", "learn makes me happy"),
        ("li si", 9, "grade 4", "li er", "wan jiu", "i just want go outside"),
        ("san zhang", 10, "grade 5", "", "", ""),
        ("li sisi", 11, "grade 6", "li ba", "li liuliu", "")
    """
    
    sql """sync"""

    order_qt_match_1 """
        SELECT * FROM test_nereids_match_select WHERE name match 'zhang san';
    """

    order_qt_match_2 """
        SELECT * FROM test_nereids_match_select WHERE name match 'zhang li';
    """

    order_qt_match_3 """
        SELECT * FROM test_nereids_match_select WHERE name match 'zhang';
    """

    order_qt_match_4 """
        SELECT * FROM test_nereids_match_select WHERE name match 'sisi';
    """

    order_qt_match_5 """
        SELECT * FROM test_nereids_match_select WHERE name match 'zhang li' and grade = 'grade 5';
    """

    order_qt_match_6 """
        SELECT fatherName, matherName FROM test_nereids_match_select WHERE name match 'zhang' or grade = 'grade 6';
    """

    order_qt_match_7 """
        SELECT * FROM test_nereids_match_select WHERE name match 'zhang' and selfComment match 'happy';
    """

    order_qt_match_any_1 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'zhang san';
    """

    order_qt_match_any_2 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'zhang li';
    """

    order_qt_match_any_3 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'zhang';
    """

    order_qt_match_any_4 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'sisi';
    """

    order_qt_match_any_5 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'zhang li' and grade = 'grade 5';
    """

    order_qt_match_any_6 """
        SELECT fatherName, matherName FROM test_nereids_match_select WHERE name match_any 'zhang' or grade = 'grade 6';
    """

    order_qt_match_any_7 """
        SELECT * FROM test_nereids_match_select WHERE name match_any 'zhang' and selfComment match_any 'happy';
    """

    order_qt_match_all_1 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'zhang san';
    """

    order_qt_match_all_2 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'zhang yi';
    """

    order_qt_match_all_3 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'li si';
    """

    order_qt_match_all_4 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'sisi';
    """

    order_qt_match_all_5 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'zhang yi' and grade = 'grade 5';
    """

    order_qt_match_all_6 """
        SELECT fatherName, matherName FROM test_nereids_match_select WHERE name match_all 'zhang yi' or grade = 'grade 6';
    """

    order_qt_match_all_7 """
        SELECT * FROM test_nereids_match_select WHERE name match_all 'zhang' and selfComment match_all 'Class activists';
    """

    order_qt_match_phrase_1 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'zhang san';
    """

    order_qt_match_phrase_2 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'zhang yi';
    """

    order_qt_match_phrase_3 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'li si';
    """

    order_qt_match_phrase_4 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'sisi li';
    """

    order_qt_match_phrase_5 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'zhang yi' and grade = 'grade 5';
    """

    order_qt_match_phrase_6 """
        SELECT fatherName, matherName FROM test_nereids_match_select WHERE name match_phrase 'zhang yi' or grade = 'grade 6';
    """

    order_qt_match_phrase_7 """
        SELECT * FROM test_nereids_match_select WHERE name match_phrase 'zhang' and selfComment match_phrase 'want go outside';
    """

    def variables = sql "show variables"
    def variableString = variables.stream()
            .map { it.toString() }
            .collect(Collectors.joining("\n"))
    logger.info("Variables:\n${variableString}")

    sql "set enable_fold_constant_by_be=false"

    explain {
        sql """
        select *
        from test_nereids_match_select a
        left join
        test_nereids_match_select b
        on a.age = b.age
        where b.name match_any 'zhang'
        """

        contains("INNER JOIN")
    }

    order_qt_match_join """
        select *
        from test_nereids_match_select a
        left join
        test_nereids_match_select b
        on a.age = b.age
        where b.name match_any 'zhang'
    """
}

