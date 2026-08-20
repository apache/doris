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

import org.junit.Assert

suite("mv_stats_calibration", "mv") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"

    sql "drop materialized view if exists mv_calib_mv"
    sql "drop table if exists mv_calib_t"
    sql "drop table if exists mv_calib_seed"

    sql """
        CREATE TABLE mv_calib_t (k int, v int) DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES ('replication_num'='1')
    """
    sql """
        CREATE TABLE mv_calib_seed (n int) DISTRIBUTED BY HASH(n) BUCKETS 1
        PROPERTIES ('replication_num'='1')
    """
    sql "insert into mv_calib_seed values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)"
    // 1000 rows, k in [0, 1000)
    sql """
        insert into mv_calib_t
        select a.n*100 + b.n*10 + c.n, (a.n*100 + b.n*10 + c.n) * 2
        from mv_calib_seed a cross join mv_calib_seed b cross join mv_calib_seed c
    """
    sql "ANALYZE TABLE mv_calib_t WITH SYNC"
    sql "ANALYZE TABLE mv_calib_seed WITH SYNC"

    sql """
        CREATE MATERIALIZED VIEW mv_calib_mv BUILD IMMEDIATE REFRESH AUTO ON COMMIT
        DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num'='1')
        AS (SELECT k, count(v) FROM mv_calib_t GROUP BY k)
    """
    waitingMTMVTaskFinishedByMvName("mv_calib_mv")

    // query result is correct and the mv is selected without calibration
    order_qt_base "select k, count(v) from mv_calib_t group by k order by k"
    mv_rewrite_all_success_without_check_chosen(
            "select k, count(v) from mv_calib_t group by k", ["mv_calib_mv"])

    // insert 1000 more rows with new k values in [1000, 2000) without re-analyzing the base table,
    // so the mv actual row count (2000) diverges from the stale base stats based estimate (about 1000)
    sql """
        insert into mv_calib_t
        select 1000 + a.n*100 + b.n*10 + c.n, (1000 + a.n*100 + b.n*10 + c.n) * 2
        from mv_calib_seed a cross join mv_calib_seed b cross join mv_calib_seed c
    """
    waitingMTMVTaskFinishedByMvName("mv_calib_mv")
    def mvRows = sql "select count(*) from mv_calib_mv"
    Assert.assertEquals(2000, mvRows[0][0])

    // without calibration, the mv scan estimate still reflects the stale base stats
    explain {
        sql "memo plan select k, count(v) from mv_calib_t group by k"
        check { result ->
            long mvEstRows = 0
            result.eachLine { line ->
                if (line.contains("calib_mv") && line.contains("LogicalOlapScan")) {
                    def matcher = line =~ /estRows=([\d,]+)/
                    if (matcher.find()) {
                        mvEstRows = matcher.group(1).replaceAll(",", "").toLong()
                    }
                }
            }
            Assert.assertTrue("mv scan estRows should be the stale estimate without calibration, but got "
                    + mvEstRows, mvEstRows < 2000)
        }
    }

    // enable stats calibration, the mv scan estimate should be calibrated to the actual row count.
    // poll with retry because the be reported row count of the mv may lag behind the refresh
    sql "SET enable_materialized_view_stats_calibration=true"
    long calibratedRows = 0
    for (int retry = 0; retry < 30 && calibratedRows < 2000; retry++) {
        explain {
            sql "memo plan select k, count(v) from mv_calib_t group by k"
            check { result ->
                result.eachLine { line ->
                    if (line.contains("calib_mv") && line.contains("LogicalOlapScan")) {
                        def matcher = line =~ /estRows=([\d,]+)/
                        if (matcher.find()) {
                            calibratedRows = matcher.group(1).replaceAll(",", "").toLong()
                        }
                    }
                }
            }
        }
        if (calibratedRows < 2000) {
            Thread.sleep(1000)
        }
    }
    Assert.assertTrue("mv scan estRows should be calibrated to the actual row count, but got "
            + calibratedRows, calibratedRows >= 2000)

    // query result is still correct and the mv is still selected with calibration
    order_qt_calib "select k, count(v) from mv_calib_t group by k order by k"
    mv_rewrite_all_success_without_check_chosen(
            "select k, count(v) from mv_calib_t group by k", ["mv_calib_mv"])

    sql "SET enable_materialized_view_stats_calibration=false"
}
