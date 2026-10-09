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

suite("test_ivm_task_query_id", "mtmv") {
    sql """drop materialized view if exists ivm_task_query_id_mv;"""
    sql """drop table if exists ivm_task_query_id_base;"""

    sql """
        CREATE TABLE ivm_task_query_id_base (
            id BIGINT,
            value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """INSERT INTO ivm_task_query_id_base VALUES (1, 10), (2, 20);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_task_query_id_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, value FROM ivm_task_query_id_base;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_task_query_id_mv COMPLETE;"""
    waitingMTMVTaskFinishedByMvName("ivm_task_query_id_mv")

    def completeTask = sql_return_maparray """
        SELECT Status, LastQueryId
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_task_query_id_mv'
        ORDER BY CreateTime DESC, TaskId DESC
        LIMIT 1
    """
    assertEquals("SUCCESS", completeTask[0].Status.toString())
    assertTrue(completeTask[0].LastQueryId != null
            && !["", "\\N"].contains(completeTask[0].LastQueryId.toString()),
            "COMPLETE refresh LastQueryId should not be empty: ${completeTask}")

    sql """INSERT INTO ivm_task_query_id_base VALUES (3, 30);"""
    sql """REFRESH MATERIALIZED VIEW ivm_task_query_id_mv INCREMENTAL;"""
    waitingMTMVTaskFinishedByMvName("ivm_task_query_id_mv")

    def incrementalTask = sql_return_maparray """
        SELECT Status, LastQueryId
        FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_task_query_id_mv'
        ORDER BY CreateTime DESC, TaskId DESC
        LIMIT 1
    """
    assertEquals("SUCCESS", incrementalTask[0].Status.toString())
    assertTrue(incrementalTask[0].LastQueryId != null
            && !["", "\\N"].contains(incrementalTask[0].LastQueryId.toString()),
            "INCREMENTAL refresh LastQueryId should not be empty: ${incrementalTask}")
}
