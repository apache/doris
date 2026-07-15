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

suite("test_ivm_bitmap_agg_1") {
    sql """drop materialized view if exists test_ivm_bitmap_agg_1_mv;"""
    sql """drop table if exists test_ivm_bitmap_agg_1_t;"""

    sql """
        CREATE TABLE test_ivm_bitmap_agg_1_t (
            id INT,
            k INT,
            b BITMAP,
            keep_bitmap TINYINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_bitmap_agg_1_t VALUES
            (1, 1, bitmap_from_string('1,2'), 1),
            (2, 1, bitmap_from_string('2,3'), 1),
            (3, 2, bitmap_from_string('10'), 1);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT k,
               BITMAP_UNION(IF(keep_bitmap = 1, b, NULL)) AS bu,
               BITMAP_UNION_COUNT(IF(keep_bitmap = 1, b, NULL)) AS buc
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k;
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    advance_ivm_stream_offset("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_complete """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_complete_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """INSERT INTO test_ivm_bitmap_agg_1_t VALUES (4, 1, bitmap_from_string('3,4'), 1);"""
    sql """INSERT INTO test_ivm_bitmap_agg_1_t VALUES (5, 3, bitmap_from_string('100,101'), 1);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_incremental """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_incremental_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    advance_ivm_stream_offset("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_complete2 """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_complete2_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """DELETE FROM test_ivm_bitmap_agg_1_t WHERE id = 3;"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_delete_group """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """

    sql """INSERT INTO test_ivm_bitmap_agg_1_t VALUES (3, 2, bitmap_from_string('10'), 1);"""
    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    advance_ivm_stream_offset("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_delete_group_complete """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_delete_group_complete_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """DELETE FROM test_ivm_bitmap_agg_1_t WHERE id = 6;"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_delete_null """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_delete_null_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """DELETE FROM test_ivm_bitmap_agg_1_t WHERE id = 8;"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_delete_empty """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_delete_empty_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """

    sql """DELETE FROM test_ivm_bitmap_agg_1_t WHERE id = 2;"""
    sql """INSERT INTO test_ivm_bitmap_agg_1_t VALUES (7, 2, bitmap_from_string('10,11'), 1);"""
    Thread.sleep(1000)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv INCREMENTAL"""

    def showTasksSql = """
        SELECT Status, ErrorMsg FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'test_ivm_bitmap_agg_1_mv'
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
    logger.info("BITMAP delete INCREMENTAL task status: " + taskStatus
            + ", error: " + taskResult[0].ErrorMsg)
    assertEquals("FAILED", taskStatus,
            "Expected INCREMENTAL to fail when deleting non-empty BITMAP aggregate input")
    def errorMsg = taskResult[0].ErrorMsg.toString()
    assertTrue(errorMsg.contains("BITMAP_AGG_DELETE") || errorMsg.contains("deleted row affects BITMAP aggregate"),
            "Error should mention BITMAP_AGG_DELETE or BITMAP delete guard but got: " + errorMsg)

    sql """REFRESH MATERIALIZED VIEW test_ivm_bitmap_agg_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_bitmap_agg_1_mv")
    advance_ivm_stream_offset("test_ivm_bitmap_agg_1_mv")
    order_qt_bitmap_delete_fallback """
        SELECT k, bitmap_to_string(bu), buc
        FROM test_ivm_bitmap_agg_1_mv
    """
    order_qt_bitmap_delete_fallback_source """
        SELECT k,
               bitmap_to_string(bitmap_union(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))),
               bitmap_union_count(IF(keep_bitmap = 1, bitmap_from_string(bitmap_to_string(b)), NULL))
        FROM test_ivm_bitmap_agg_1_t
        GROUP BY k
    """
}
