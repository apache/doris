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

suite("test_predicate") {
    def table1 = "test_predicate"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      `k1` decimalv3(38, 18) NULL COMMENT "",
      `k2` decimalv3(38, 18) NULL COMMENT "",
      `k3` decimalv3(38, 18) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values(1.1,1.2,1.3),
            (1.2,1.2,1.3),
            (1.5,1.2,1.3)
    """
    qt_select1 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(2,1))"

    qt_select2 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ 1 FROM ${table1} WHERE CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(2,1));"
    qt_select3 "SELECT * FROM ${table1} WHERE k1 != 1.1 ORDER BY k1"

    // decimal256
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"
    qt_select4 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(76,1))"
    qt_select5 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ 1 FROM ${table1} WHERE CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS FLOAT) = CAST(1.2 AS decimal(76,1));"
    qt_select6 "SELECT * FROM ${table1} WHERE k1 != cast(1.1 as decimalv3(76, 1)) ORDER BY k1"
    sql "drop table if exists ${table1}"

    qt_select256_1 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) > cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_2 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10)) > cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_3 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) >= cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_4 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999997 as decimalv3(76,10)) >= cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"

    qt_select256_5 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10)) < cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10))"
    qt_select256_6 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) < cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_7 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10)) <= cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_8 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) <= cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"

    qt_select256_9 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) = cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10))"
    qt_select256_10 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) = cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"

    qt_select256_11 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) != cast(999999999999999999999999999999999999999999999999999999999999999999.9999999998 as decimalv3(76,10))"
    qt_select256_12 "SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */ cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10)) != cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10))"


    sql "DROP TABLE IF EXISTS `test_predicate_128_1`";
    sql """
    CREATE TABLE IF NOT EXISTS `test_predicate_128_1` (
      `k1` decimalv3(38, 6) NULL COMMENT "",
      `k2` decimalv3(38, 6) NULL COMMENT "",
      `k3` decimalv3(38, 6) NULL COMMENT ""
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into test_predicate_128_1 values(1, 99999999999999999999999999999999.999999, 99999999999999999999999999999999.999999),
            (2, 49999999999999999999999999999999.999999, 49999999999999999999999999999999.999999),
            (3, 33333333333333333333333333333333.333333, 33333333333333333333333333333333.333333),
            (4.444444, 2.222222, 3.333333);"""
    qt_decimal256_select_all "select * from test_predicate_128_1 order by k1, k2;"
    qt_decimal256_predicate_0 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) > (cast(33333333333333333333333333333333.333333 as decimalv3(76,7))) order by k1, k2;"
    qt_decimal256_predicate_1 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) >= (cast(999999999999999999999999999999990.999999 as decimalv3(76,6)) / 10)order by k1, k2;"

    qt_decimal256_predicate_2 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) < (cast(49999999999999999999999999999999.999999 as decimalv3(76,7))) order by k1, k2;"
    qt_decimal256_predicate_3 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) <= (cast(33333333333333333333333333333333.333333 as decimalv3(76,7))) order by k1, k2;"

    qt_decimal256_predicate_4 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) = (cast(99999999999999999999999999999999.999999 as decimalv3(76,7))) order by k1, k2;"
    qt_decimal256_predicate_5 "select * from test_predicate_128_1 where cast(k2 as decimalv3(76, 6)) != (cast(99999999999999999999999999999999.999999 as decimalv3(76,7))) order by k1, k2;"

}
