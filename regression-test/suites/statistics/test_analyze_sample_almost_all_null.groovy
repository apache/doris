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

// Regression for issue #64122: ColStatsData.isValid() falsely rejects
// sampled column statistics when a column is (almost) all NULL.
//
// On a Unique-Key MoW table where column v is almost entirely NULL but
// has one surviving non-null value, sample analyze produces
//   ndv=0 (estimated), min=max='x' (full-scan), nullCount != count
// which trips the second isValid() guard. Before the fix, runQuery()
// threw and aborted the whole analyze job; after the fix the row is
// written and toColumnStatistic() falls back to UNKNOWN at read time.
suite("test_analyze_sample_almost_all_null") {

    def wait_row_count_at_least = { db, table, threshold ->
        // For Unique MoW the post-DELETE row count is non-trivial to predict
        // exactly, so we just gate on "row count is reported and large enough",
        // which is what we need to trigger the isValid() guard. count=0 would
        // short-circuit isValid() and the issue would not reproduce.
        def result = sql """show frontends;"""
        def host
        def port
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "true") {
                host = result[i][1]
                port = result[i][4]
            }
        }
        def tokens = context.config.jdbcUrl.split('/')
        def url = tokens[0] + "//" + host + ":" + port
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql """use ${db}"""
            for (int i = 0; i < 120; i++) {
                Thread.sleep(5000)
                result = sql """SHOW DATA FROM ${table};"""
                logger.info("SHOW DATA FROM ${table}: " + result)
                // Sum the row-count column across all rows returned by SHOW DATA.
                // Layout: rows are per-partition + a Total row at the end. The
                // row-count column index is 4 (same assumption as the existing
                // test_analyze_all_null suite).
                def total = 0L
                for (int r = 0; r < result.size(); r++) {
                    def v = result[r][4]
                    if (v == null) {
                        continue
                    }
                    try {
                        total += Long.parseLong(v.toString())
                    } catch (NumberFormatException ignored) {
                        // "Total" row may already be a formatted string; skip.
                    }
                }
                if (total >= threshold) {
                    return
                }
            }
            throw new Exception("Row count report timeout for ${db}.${table}, "
                    + "threshold=" + threshold + ", last result=" + result)
        }
    }

    sql """drop database if exists regression_test_analyze_sample_almost_all_null"""
    sql """create database regression_test_analyze_sample_almost_all_null"""
    sql """use regression_test_analyze_sample_almost_all_null"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE tbl_del_big (
            k INT NOT NULL,
            v VARCHAR(64) NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 64
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    // 2M rows with v=NULL; then 1 row with v='x' overwriting (k=1, NULL).
    // DELETE half of the NULL rows so the surviving data set is ~1M rows,
    // with exactly one non-null v value.
    sql """INSERT INTO tbl_del_big SELECT number, NULL FROM numbers("number"="2000000")"""
    sql """INSERT INTO tbl_del_big SELECT number * 64 + 1, 'x' FROM numbers("number"="1")"""
    sql """DELETE FROM tbl_del_big WHERE k % 2 = 0 AND v IS NULL"""

    wait_row_count_at_least("regression_test_analyze_sample_almost_all_null",
            "tbl_del_big", 500000L)

    sql """ANALYZE TABLE tbl_del_big WITH SAMPLE PERCENT 1 WITH SYNC"""

    def result = sql """show column stats tbl_del_big"""

    // k (NOT NULL) always produces valid sampled stats. Whether v also survives
    // isValid() depends on whether the single 'x' row lands in one of the randomly
    // chosen sample tablets (sampled -> ndv ~ 1, valid; not sampled -> ndv = 0 with
    // full-scan min/max = 'x', invalid). So only assert on k and a loose row count.
    assertTrue(result.size() >= 1)
    assertTrue(result.any { it[0] == "k" })

    // Deterministically construct the issue #64122 invalid pattern. SET STATS writes
    // the row into the statistics table directly (no isValid check on that path) and
    // syncColStats invalidates the cache entry. The next read goes through
    // ColumnStatistic.fromResultRow, whose isValid() guard returns UNKNOWN for
    // ndv=0 + min/max!=null + nullCount!=count, so the optimizer must see unknown.
    sql """ALTER TABLE tbl_del_big MODIFY COLUMN v SET STATS (
            'row_count'='1000000', 'ndv'='0', 'num_nulls'='999999',
            'data_size'='8000000', 'min_value'='x', 'max_value'='x')"""

    explain {
        sql("select * from tbl_del_big")
        contains("planned with unknown column statistics")
    }

    explain {
        sql("memo plan select * from tbl_del_big")
        contains("v#1 -> unknown(")
    }

    sql """drop database if exists regression_test_analyze_sample_almost_all_null"""
}
