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

suite("test_decimal256_load") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    // test insert
    sql "DROP TABLE IF EXISTS `test_decimal256_insert`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_insert` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
      insert into test_decimal256_insert values
         (2, 499999999999999999999999999999999999999999999999999999999999999999.9999999999, 49999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
         (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
         (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
         (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
         (6, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (7, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
         (0, 0, 0),
         (99999999999999999999999999999.999999999, 1, 1),
         (-99999999999999999999999999999.999999999, 1, 1),
         (9999999999999999999999999999999999999999999999999999999999999999999.999999999, 1, 1),
         (-9999999999999999999999999999999999999999999999999999999999999999999.999999999, 1, 1),
         (4999999999999999999999999999999999999999999999999999999999999999999.999999999, 1, 1),
         (-4999999999999999999999999999999999999999999999999999999999999999999.999999999, 1, 1),
         (null, null, null);
    """
    sql "sync"
    qt_decimal256_insert_select_all0 "select * from test_decimal256_insert order by 1,2,3;"

    // test stream load
    sql "DROP TABLE IF EXISTS `test_decimal256_load`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_load` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL default '999999999999999999999999999999999999999999999999999999999999999999.9999999999' COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    streamLoad {
        // you can skip db declaration, because a default db has already been
        // specified in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table "test_decimal256_load"

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """test_decimal256_load.csv"""

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(19, json.NumberTotalRows)
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.LoadBytes > 0)
        }
    }
    qt_select "select * from test_decimal256_load order by 1,2,3;"

    qt_select_key_eq0 """
       select * from test_decimal256_load where k1 = 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_eq1 """
       select * from test_decimal256_load where k1 = 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_eq2 """
       select * from test_decimal256_load where k1 = -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_eq3 """
       select * from test_decimal256_load where k1 = -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_eq4 """
       select * from test_decimal256_load where k1 = 0 order by 1, 2, 3;
    """
    qt_select_key_eq5 """
       select * from test_decimal256_load where k1 = null order by 1, 2, 3;
    """

    qt_select_key_neq0 """
       select * from test_decimal256_load where k1 != 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_neq1 """
       select * from test_decimal256_load where k1 != 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_neq2 """
       select * from test_decimal256_load where k1 != -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_neq3 """
       select * from test_decimal256_load where k1 != -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_neq4 """
       select * from test_decimal256_load where k1 != 0 order by 1, 2, 3;
    """
    qt_select_key_neq5 """
       select * from test_decimal256_load where k1 != null order by 1, 2, 3;
    """

    qt_select_key_gt0 """
       select * from test_decimal256_load where k1 > 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_gt1 """
       select * from test_decimal256_load where k1 > 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_gt2 """
       select * from test_decimal256_load where k1 > -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_gt3 """
       select * from test_decimal256_load where k1 > -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_gt4 """
       select * from test_decimal256_load where k1 > 0 order by 1, 2, 3;
    """
    qt_select_key_gt5 """
       select * from test_decimal256_load where k1 > null order by 1, 2, 3;
    """

    qt_select_key_ge0 """
       select * from test_decimal256_load where k1 >= 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_ge1 """
       select * from test_decimal256_load where k1 >= 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_ge2 """
       select * from test_decimal256_load where k1 >= -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_ge3 """
       select * from test_decimal256_load where k1 >= -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_ge4 """
       select * from test_decimal256_load where k1 >= 0 order by 1, 2, 3;
    """
    qt_select_key_ge5 """
       select * from test_decimal256_load where k1 >= null order by 1, 2, 3;
    """

    qt_select_key_lt0 """
       select * from test_decimal256_load where k1 < 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_lt1 """
       select * from test_decimal256_load where k1 < 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_lt2 """
       select * from test_decimal256_load where k1 < -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_lt3 """
       select * from test_decimal256_load where k1 < -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_lt4 """
       select * from test_decimal256_load where k1 < 0 order by 1, 2, 3;
    """
    qt_select_key_lt5 """
       select * from test_decimal256_load where k1 < null order by 1, 2, 3;
    """

    qt_select_key_le0 """
       select * from test_decimal256_load where k1 <= 9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_le1 """
       select * from test_decimal256_load where k1 <= 4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_le2 """
       select * from test_decimal256_load where k1 <= -4999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_le3 """
       select * from test_decimal256_load where k1 <= -9999999999999999999999999999999999999999999999999999999999999999999.999999999 order by 1, 2, 3;
    """
    qt_select_key_le4 """
       select * from test_decimal256_load where k1 <= 0 order by 1, 2, 3;
    """
    qt_select_key_le5 """
       select * from test_decimal256_load where k1 <= null order by 1, 2, 3;
    """

    qt_select_key_in0 """
       select * from test_decimal256_load where k1 in (9999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_in1 """
       select * from test_decimal256_load where k1 in (-9999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_in2 """
       select * from test_decimal256_load where k1 in(4999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_in3 """
       select * from test_decimal256_load where k1 in(-4999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_in4 """
       select * from test_decimal256_load where k1 in(0) order by 1, 2, 3;
    """
    qt_select_key_in5 """
       select * from test_decimal256_load where k1 in (
        9999999999999999999999999999999999999999999999999999999999999999999.999999999,
        -9999999999999999999999999999999999999999999999999999999999999999999.999999999,
        4999999999999999999999999999999999999999999999999999999999999999999.999999999,
        -4999999999999999999999999999999999999999999999999999999999999999999.999999999,
        0
        ) order by 1, 2, 3;
    """
    qt_select_key_in6 """
       select * from test_decimal256_load where k1 in(null) order by 1, 2, 3;
    """
    qt_select_key_in7 """
       select * from test_decimal256_load where k1 in(0, null) order by 1, 2, 3;
    """

    qt_select_key_notin0 """
       select * from test_decimal256_load where k1 not in (9999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_notin1 """
       select * from test_decimal256_load where k1 not in (-9999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_notin2 """
       select * from test_decimal256_load where k1 not in(4999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_notin3 """
       select * from test_decimal256_load where k1 not in(-4999999999999999999999999999999999999999999999999999999999999999999.999999999) order by 1, 2, 3;
    """
    qt_select_key_notin4 """
       select * from test_decimal256_load where k1 not in(0) order by 1, 2, 3;
    """
    qt_select_key_notin5 """
       select * from test_decimal256_load where k1 not in (
        9999999999999999999999999999999999999999999999999999999999999999999.999999999,
        -9999999999999999999999999999999999999999999999999999999999999999999.999999999,
        4999999999999999999999999999999999999999999999999999999999999999999.999999999,
        -4999999999999999999999999999999999999999999999999999999999999999999.999999999,
        0
        ) order by 1, 2, 3;
    """
    qt_select_key_notin6 """
       select * from test_decimal256_load where k1 not in(null) order by 1, 2, 3;
    """
    qt_select_key_notin7 """
       select * from test_decimal256_load where k1 not in(0, null) order by 1, 2, 3;
    """

    qt_select_key_is_null """
       select * from test_decimal256_load where k1 is null order by 1, 2, 3;
    """
    qt_select_key_is_not_null """
       select * from test_decimal256_load where k1 is not null order by 1, 2, 3;
    """

   qt_sql_eq_1 """
       select * from test_decimal256_load where k2 = 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   qt_sql_neq_1 """
       select * from test_decimal256_load where k2 != 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   qt_sql_gt_1 """
       select * from test_decimal256_load where k2 > 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   qt_sql_ge_1 """
       select * from test_decimal256_load where k2 >= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   qt_sql_lt_1 """
       select * from test_decimal256_load where k2 < 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   qt_sql_le_1 """
       select * from test_decimal256_load where k2 <= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   sql "DROP TABLE IF EXISTS `test_decimal256_load2`"
   sql """
   CREATE TABLE IF NOT EXISTS `test_decimal256_load2` (
     `k1` decimalv3(76, 9) NULL COMMENT "",
     `k2` decimalv3(76, 10) NULL COMMENT "",
     `k3` decimalv3(76, 11) NULL COMMENT ""
   ) ENGINE=OLAP
   DUPLICATE KEY(`k1`)
   DISTRIBUTED BY HASH(`k1`) BUCKETS 8
   PROPERTIES (
   "replication_allocation" = "tag.location.default: 1"
   );
   """
   sql "insert into test_decimal256_load2 select * from test_decimal256_load;"
   sql "sync"
   qt_select_2 "select * from test_decimal256_load2 order by 1,2,3;"
   qt_sql_insert_select_eq_1 """
       select * from test_decimal256_load2 where k2 = 999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
   """

   // test default value
   sql "insert into test_decimal256_load(k1,k3) values (0, 0), (99999, 99999), (-99999, -99999);"
   sql "sync"
   qt_sql_select_insert_default0 "select * from test_decimal256_load order by k1,k2,k3;"
   qt_sql_select_insert_default1 "select * from test_decimal256_load where k1 = 99999;"
   qt_sql_select_insert_default2 "select * from test_decimal256_load where k1 = -99999;"
}