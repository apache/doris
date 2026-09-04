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

suite("test_ivm_strict_failure_partition_atomicity", "nonConcurrent") {
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_strict_atomicity_mv"""
    sql """DROP TABLE IF EXISTS ivm_strict_atomicity_t"""
    sql """
        CREATE TABLE ivm_strict_atomicity_t (
            dt DATE NOT NULL,
            id BIGINT NOT NULL,
            v INT
        )
        UNIQUE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01')),
            PARTITION p3 VALUES [('2026-03-01'), ('2026-04-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_strict_atomicity_t VALUES
            ('2026-01-10', 1, 10),
            ('2026-02-10', 2, 20),
            ('2026-03-10', 3, 30)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_strict_atomicity_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, id, v FROM ivm_strict_atomicity_t
    """
    sql """REFRESH MATERIALIZED VIEW ivm_strict_atomicity_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_strict_atomicity_mv")
    order_qt_before_ddl """
        SELECT dt, id, v FROM ivm_strict_atomicity_mv ORDER BY dt, id
    """

    sql """TRUNCATE TABLE ivm_strict_atomicity_t PARTITION(p1)"""
    sql """ALTER TABLE ivm_strict_atomicity_t DROP PARTITION p2"""
    def previousTaskId = sql("""
        SELECT TaskId FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_strict_atomicity_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """)[0][0].toString()

    sql """REFRESH MATERIALIZED VIEW ivm_strict_atomicity_mv INCREMENTAL"""
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        def task = sql_return_maparray("""
            SELECT TaskId, Status FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_strict_atomicity_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """)
        return !task.isEmpty()
                && task[0].TaskId.toString() != previousTaskId
                && task[0].Status.toString() != 'PENDING'
                && task[0].Status.toString() != 'RUNNING'
    })
    order_qt_strict_task """
        SELECT Status, IvmFallbackReason,
               ErrorMsg LIKE '%baseline rebuild is pending%'
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_strict_atomicity_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    order_qt_after_strict_failure """
        SELECT dt, id, v FROM ivm_strict_atomicity_mv ORDER BY dt, id
    """

    sql """REFRESH MATERIALIZED VIEW ivm_strict_atomicity_mv AUTO"""
    waitingMTMVTaskFinishedByMvName("ivm_strict_atomicity_mv")
    order_qt_after_auto_recovery """
        SELECT dt, id, v FROM ivm_strict_atomicity_mv ORDER BY dt, id
    """
}
