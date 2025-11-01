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

suite("test_str_to_date") {
    sql """ DROP TABLE IF EXISTS test_str_to_date_db """

    sql """ 
        CREATE TABLE IF NOT EXISTS test_str_to_date_db (
              `id` INT NULL COMMENT "",
              `s1` String NULL COMMENT "",
              `s2` String NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO test_str_to_date_db VALUES(1,'2019-12-01', 'yyyy-MM-dd');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(2,'20201203', 'yyyyMMdd');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(3,'2020-12-03 11:45:14', 'yyyy-MM-dd HH:mm:ss');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(4,null, 'yyyy-MM-dd');"""
    sql """ INSERT INTO test_str_to_date_db VALUES(5,'2019-12-01', null);"""
    sql """ INSERT INTO test_str_to_date_db VALUES(6,null, null);"""
    // sql """ INSERT INTO test_str_to_date_db VALUES(7,'无效日期', 'yyyy-MM-dd');"""

    qt_select1 """
        select id, s1, s2, STR_TO_DATE(s1, s2) from test_str_to_date_db 
        where id < 7 order by id;
    """

    qt_const_test1 """
        SELECT STR_TO_DATE('2019-12-01', 'yyyy-MM-dd');
    """
    qt_const_test2 """
        SELECT STR_TO_DATE(null, 'yyyy-MM-dd');
    """
    qt_const_test3 """
        SELECT STR_TO_DATE('2019-12-01', null);
    """
    qt_const_test4 """
        SELECT STR_TO_DATE(null, null);
    """
    test {
        sql "select STR_TO_DATE('无效日期', 'yyyy-MM-dd')"
        exception "is invalid"
    }

    qt_const_test5 """
        SELECT /*+SET_VAR(debug_skip_fold_constant=false) */STR_TO_DATE('09:30:17', '%h:%i:%s');
    """

    qt_const_test6 """
        SELECT /*+SET_VAR(debug_skip_fold_constant=false) */STR_TO_DATE('09:30:17', '%h:%i:%s');
    """

    qt_short_1 " select STR_TO_DATE('2023', '%Y') "
    qt_short_2 " select STR_TO_DATE(null, '%Y') "
    qt_short_3 " select STR_TO_DATE('2023', null) "
    qt_short_4 " select STR_TO_DATE(null, null) "
    qt_short_5 " select STR_TO_DATE('202501', '%Y%m') "
    qt_short_6 " select STR_TO_DATE('202503', '%Y%m') "

    test {
        sql """
                SELECT id, STR_TO_DATE(s1, '%Y-%m-%d') as result from test_str_to_date_db order by id;
            """
        exception "is invalid"
    }
    qt_select_from_table2 """
        SELECT id, STR_TO_DATE(s1, s2) as result from test_str_to_date_db order by id;
    """

    check_fold_consistency "STR_TO_DATE('2019-12-01', 'yyyy-MM-dd')"
    check_fold_consistency "STR_TO_DATE(null, 'yyyy-MM-dd')"
    check_fold_consistency "STR_TO_DATE('2019-12-01', null)"
    check_fold_consistency "STR_TO_DATE(null, null)"

    test {
        sql """
            SELECT STR_TO_DATE('  ', '%Y-%m-%d %H:%i:%s');
        """
        exception "is invalid"
    }
    test {
        sql "SELECT str_to_date('2022-09-18 00:00:59','%Y-%m-%d %H:%M:%S')"
        exception "%Y-%m-%d %H:%M:%S is invalid"
    }
}
