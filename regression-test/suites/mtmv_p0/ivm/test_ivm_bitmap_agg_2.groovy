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

suite("test_ivm_bitmap_agg_2") {
    sql """drop materialized view if exists test_ivm_bitmap_agg_2_mv;"""
    sql """drop table if exists test_ivm_bitmap_agg_2_t;"""
    sql """drop materialized view if exists test_ivm_bitmap_agg_2_all_mv;"""
    sql """drop table if exists test_ivm_bitmap_agg_2_all_t;"""

    sql """
        CREATE TABLE test_ivm_bitmap_agg_2_t (
            id INT,
            b BITMAP,
            keep_bitmap TINYINT,
            binlog_op TINYINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_bitmap_agg_2_t VALUES
            (1, bitmap_from_string('1,2'), 1, 0),
            (2, bitmap_from_string('2,3'), 1, 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT BITMAP_UNION(IF(keep_bitmap = 1, b, NULL)) AS bu,
               BITMAP_UNION_COUNT(IF(keep_bitmap = 1, b, NULL)) AS buc
        FROM test_ivm_bitmap_agg_2_t;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_mv")
    order_qt_scalar_complete """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_mv
    """
    order_qt_scalar_complete_source """
        SELECT bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_2_t
    """

    sql """INSERT INTO test_ivm_bitmap_agg_2_t VALUES (3, bitmap_from_string('3,4'), 1, 0);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_mv")
    order_qt_scalar_incremental """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_mv
    """
    order_qt_scalar_incremental_source """
        SELECT bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_2_t
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_mv")

    sql """INSERT INTO test_ivm_bitmap_agg_2_t VALUES (4, bitmap_from_string('999'), 0, 1);"""
    sql """INSERT INTO test_ivm_bitmap_agg_2_t VALUES (5, bitmap_from_string(''), 1, 1);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_mv")
    order_qt_scalar_delete_null_empty """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_mv
    """
    order_qt_scalar_delete_null_empty_source """
        SELECT bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_2_t
    """

    sql """INSERT INTO test_ivm_bitmap_agg_2_t VALUES (2, bitmap_from_string('2,3'), 1, 1);"""
    sql """INSERT INTO test_ivm_bitmap_agg_2_t VALUES (6, bitmap_from_string('5'), 1, 0);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv INCREMENTAL"""

    def showTasksSql = """
        SELECT Status, ErrorMsg FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_bitmap_agg_2_mv'
        ORDER BY CreateTime DESC LIMIT 1
    """
    def taskResult
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        taskResult = sql_return_maparray(showTasksSql)
        if (taskResult.isEmpty()) {
            return false
        }
        def status = taskResult[0].Status.toString()
        return status != 'PENDING' && status != 'RUNNING'
    })

    def taskStatus = taskResult[0].Status.toString()
    logger.info("Scalar BITMAP delete INCREMENTAL task status: " + taskStatus
            + ", error: " + taskResult[0].ErrorMsg)
    assertEquals("FAILED", taskStatus,
            "Expected scalar INCREMENTAL to fail when deleting non-empty BITMAP aggregate input")
    def errorMsg = taskResult[0].ErrorMsg.toString()
    assertTrue(errorMsg.contains("BITMAP_AGG_DELETE") || errorMsg.contains("deleted row affects BITMAP aggregate"),
            "Error should mention BITMAP_AGG_DELETE or BITMAP delete guard but got: " + errorMsg)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_mv")
    order_qt_scalar_delete_fallback """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_mv
    """
    order_qt_scalar_delete_fallback_source """
        SELECT bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_2_t
    """

    sql """
        CREATE TABLE test_ivm_bitmap_agg_2_all_t (
            id INT,
            b BITMAP,
            binlog_op TINYINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_bitmap_agg_2_all_t VALUES
            (1, bitmap_from_string('10'), 0),
            (2, bitmap_from_string('20'), 0);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_bitmap_agg_2_all_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT BITMAP_UNION(b) AS bu,
               BITMAP_UNION_COUNT(b) AS buc
        FROM test_ivm_bitmap_agg_2_all_t;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_all_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_all_mv")
    order_qt_scalar_delete_all_complete """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_all_mv
    """

    sql """INSERT INTO test_ivm_bitmap_agg_2_all_t VALUES (1, bitmap_from_string('10'), 1);"""
    sql """INSERT INTO test_ivm_bitmap_agg_2_all_t VALUES (2, bitmap_from_string('20'), 1);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_2_all_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_2_all_mv")
    order_qt_scalar_delete_all_incremental """
        SELECT bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_2_all_mv
    """
}
