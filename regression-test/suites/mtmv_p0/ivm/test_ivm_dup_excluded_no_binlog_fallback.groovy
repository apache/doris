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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_dup_excluded_no_binlog_fallback") {
    // MOW INNER JOIN excluded-DUP (no binlog) — MOW delete falls back.
    // The excluded DUP table has no row binlog, so it has no __DORIS_ROW_LSN_COL__ and
    // keeps the legacy uuid row-id (non-deterministic). The joined MV row-id is therefore
    // non-deterministic, and the delete delta on the MOW side must fall back.

    sql """drop materialized view if exists test_ivm_dup_excl_mv;"""
    sql """drop table if exists test_ivm_dup_excl_mow;"""
    sql """drop table if exists test_ivm_dup_excl_dup;"""

    sql """
        CREATE TABLE test_ivm_dup_excl_mow (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """
    sql """
        CREATE TABLE test_ivm_dup_excl_dup (
            k1 INT,
            v2 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """INSERT INTO test_ivm_dup_excl_mow VALUES (1, 10), (2, 20);"""
    sql """INSERT INTO test_ivm_dup_excl_dup VALUES (1, 100), (2, 200);"""

    sql """
        CREATE MATERIALIZED VIEW test_ivm_dup_excl_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'excluded_trigger_tables' = 'test_ivm_dup_excl_dup'
        )
        AS
        SELECT
            test_ivm_dup_excl_mow.k1 AS k1,
            test_ivm_dup_excl_mow.v1 AS mow_v,
            test_ivm_dup_excl_dup.v2 AS dup_v
        FROM test_ivm_dup_excl_mow
        INNER JOIN test_ivm_dup_excl_dup
            ON test_ivm_dup_excl_mow.k1 = test_ivm_dup_excl_dup.k1;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_excl_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_excl_mv")
    order_qt_mow_excl_dup_after_complete """SELECT k1, mow_v, dup_v FROM test_ivm_dup_excl_mv"""

    sql """DELETE FROM test_ivm_dup_excl_mow WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_excl_mv INCREMENTAL"""

    def exclTaskSql = """
        select TaskId, Status, ErrorMsg from tasks('type'='mv')
        where MvDatabaseName = '${context.dbName}' and MvName = 'test_ivm_dup_excl_mv'
        order by CreateTime DESC limit 1
    """
    def exclTaskResult
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        exclTaskResult = sql(exclTaskSql)
        if (exclTaskResult.isEmpty()) {
            return false
        }
        def st = exclTaskResult[0][1].toString()
        return st != 'PENDING' && st != 'RUNNING'
    })
    def exclTaskStatus = exclTaskResult[0][1].toString()
    def exclErrorMsg = exclTaskResult[0][2].toString()
    assertTrue(exclTaskStatus == "FAILED",
            "Expected explicit INCREMENTAL to fail (fallback) for matched delete with non-deterministic "
                    + "excluded-DUP row-id, but got: " + exclTaskStatus)
    assertTrue(exclErrorMsg.contains("delete on non-deterministic row_id")
                    || exclErrorMsg.contains("assert_true"),
            "Expected non-deterministic row_id fallback message, but got: " + exclErrorMsg)

    sql """REFRESH MATERIALIZED VIEW test_ivm_dup_excl_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_dup_excl_mv")
    order_qt_mow_excl_dup_after_complete_recovery """SELECT k1, mow_v, dup_v FROM test_ivm_dup_excl_mv"""
}
