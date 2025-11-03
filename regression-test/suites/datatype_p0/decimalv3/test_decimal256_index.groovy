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

suite("test_decimal256_index") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        useTime = 0
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    // test zonemap index
    sql "DROP TABLE IF EXISTS `test_decimal256_zonemap_index`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_zonemap_index` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    streamLoad {
        // you can skip db declaration, because a default db has already been
        // specified in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table "test_decimal256_zonemap_index"

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """test_decimal256_zonemap_index.csv"""

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
            assertEquals(4096, json.NumberTotalRows)
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.LoadBytes > 0)
        }
    }
    // RowsStatsFiltered in profile
    qt_decimal256_zonemap_0 "select * from test_decimal256_zonemap_index where k2 < 900000000000000000000000000000000000000000000000000000000000000010.9999999999 order by k1, k2;"

    sql "DROP TABLE IF EXISTS `test_decimal256_bitmap_index`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_bitmap_index` (
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

    sql """insert into test_decimal256_bitmap_index values
            (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (2, 499999999999999999999999999999999999999999999999999999999999999999.9999999999, 49999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (6, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (7, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999);
        """
    sql "sync"

    sql """CREATE INDEX k2_bitmap_index ON test_decimal256_bitmap_index(k2) USING BITMAP;"""
    wait_for_latest_op_on_table_finish("test_decimal256_bitmap_index", 10000);
    if (!isCloudMode()) {
        sql """BUILD INDEX k2_bitmap_index ON test_decimal256_bitmap_index;"""
        wait_for_latest_op_on_table_finish("test_decimal256_bitmap_index", 10000);
        wait_for_build_index_on_partition_finish("test_decimal256_bitmap_index", 10000)
    }

    qt_sql_bitmap_index_select_all """
        select * from test_decimal256_bitmap_index order by 1,2,3;
    """
    // profile item RowsBitmapIndexFiltered
    qt_sql_eq_1 """
        select * from test_decimal256_bitmap_index where k2 = 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_eq_2 """
        select * from test_decimal256_bitmap_index where k2 = -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_eq_3 """
        select * from test_decimal256_bitmap_index where k2 = -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_neq_1 """
        select * from test_decimal256_bitmap_index where k2 != 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_neq_2 """
        select * from test_decimal256_bitmap_index where k2 != -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_neq_3 """
        select * from test_decimal256_bitmap_index where k2 != -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_gt_1 """
        select * from test_decimal256_bitmap_index where k2 > 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_gt_2 """
        select * from test_decimal256_bitmap_index where k2 > -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_gt_3 """
        select * from test_decimal256_bitmap_index where k2 > -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_ge_1 """
        select * from test_decimal256_bitmap_index where k2 >= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_ge_2 """
        select * from test_decimal256_bitmap_index where k2 >= -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_ge_3 """
        select * from test_decimal256_bitmap_index where k2 >= -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_lt_1 """
        select * from test_decimal256_bitmap_index where k2 < 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_lt_2 """
        select * from test_decimal256_bitmap_index where k2 < -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_lt_3 """
        select * from test_decimal256_bitmap_index where k2 < -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_le_1 """
        select * from test_decimal256_bitmap_index where k2 <= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_le_2 """
        select * from test_decimal256_bitmap_index where k2 <= -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_le_3 """
        select * from test_decimal256_bitmap_index where k2 <= -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    // bloom filter index
    sql "DROP TABLE IF EXISTS `test_decimal256_bf_index`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_bf_index` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "bloom_filter_columns" = "k2"
    );
    """

    sql """insert into test_decimal256_bf_index values
            (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (1, 999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (2, 499999999999999999999999999999999999999999999999999999999999999999.9999999999, 49999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (3, 333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (4, -999999999999999999999999999999999999999999999999999999999999999999.9999999999, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (5, -333333333333333333333333333333333333333333333333333333333333333333.3333333333, 33333333333333333333333333333333333333333333333333333333333333333.33333333333),
            (6, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999),
            (7, null, 99999999999999999999999999999999999999999999999999999999999999999.99999999999);
        """
    sql "sync"

    // profile item RowsBloomFilterFiltered
    qt_sql_bf_eq_1 """
        select * from test_decimal256_bf_index where k2 = 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_eq_2 """
        select * from test_decimal256_bf_index where k2 = -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_eq_3 """
        select * from test_decimal256_bf_index where k2 = -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_bf_neq_1 """
        select * from test_decimal256_bf_index where k2 != 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_neq_2 """
        select * from test_decimal256_bf_index where k2 != -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_neq_3 """
        select * from test_decimal256_bf_index where k2 != -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_bf_gt_1 """
        select * from test_decimal256_bf_index where k2 > 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_gt_2 """
        select * from test_decimal256_bf_index where k2 > -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_gt_3 """
        select * from test_decimal256_bf_index where k2 > -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_bf_ge_1 """
        select * from test_decimal256_bf_index where k2 >= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_ge_2 """
        select * from test_decimal256_bf_index where k2 >= -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_ge_3 """
        select * from test_decimal256_bf_index where k2 >= -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_bf_lt_1 """
        select * from test_decimal256_bf_index where k2 < 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_lt_2 """
        select * from test_decimal256_bf_index where k2 < -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_lt_3 """
        select * from test_decimal256_bf_index where k2 < -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

    qt_sql_bf_le_1 """
        select * from test_decimal256_bf_index where k2 <= 499999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_le_2 """
        select * from test_decimal256_bf_index where k2 <= -999999999999999999999999999999999999999999999999999999999999999999.9999999999 order by 1, 2, 3;
    """
    qt_sql_bf_le_3 """
        select * from test_decimal256_bf_index where k2 <= -333333333333333333333333333333333333333333333333333333333333333333.3333333333 order by 1, 2, 3;
    """

}