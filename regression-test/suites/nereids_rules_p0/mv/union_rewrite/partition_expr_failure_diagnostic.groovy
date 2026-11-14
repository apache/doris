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

suite("partition_expr_failure_diagnostic") {
    sql "DROP MATERIALIZED VIEW IF EXISTS partition_expr_failure_mv"
    sql "DROP TABLE IF EXISTS partition_expr_failure_t1"
    sql "DROP TABLE IF EXISTS partition_expr_failure_t2"
    sql """CREATE TABLE partition_expr_failure_t1 (
        data_date DATETIME NOT NULL, slot_id VARCHAR(255) NULL, num INT NULL
    ) DUPLICATE KEY(data_date, slot_id)
    PARTITION BY RANGE(data_date) (
        FROM ('2024-09-01') TO ('2024-09-30') INTERVAL 1 DAY
    ) DISTRIBUTED BY HASH(data_date, slot_id) BUCKETS 2
    PROPERTIES ('replication_num' = '1')"""
    sql "CREATE TABLE partition_expr_failure_t2 LIKE partition_expr_failure_t1"
    sql """INSERT INTO partition_expr_failure_t1 VALUES
        ('2024-09-12 00:20:00', 'a', 1), ('2024-09-13 00:20:00', 'b', 1)"""
    sql """INSERT INTO partition_expr_failure_t2 VALUES
        ('2024-09-12 00:20:00', 'a', 1), ('2024-09-13 00:20:00', 'b', 1)"""
    sql """CREATE MATERIALIZED VIEW partition_expr_failure_mv
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        PARTITION BY (date_trunc(data_date, 'day'))
        DISTRIBUTED BY HASH(data_date) BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT date_trunc(t1.data_date, 'day') AS data_date,
            to_date(t1.data_date) AS dt, t2.slot_id, sum(t1.num) AS num_sum
        FROM partition_expr_failure_t1 t1 INNER JOIN partition_expr_failure_t2 t2
            ON t1.data_date = t2.data_date
        GROUP BY date_trunc(t1.data_date, 'day'), to_date(t1.data_date), t2.slot_id"""
    waitingMTMVTaskFinishedByMvName("partition_expr_failure_mv")
    sql "REFRESH MATERIALIZED VIEW partition_expr_failure_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("partition_expr_failure_mv")

    // Make one partition stale so rewrite needs partition compensation.
    sql "INSERT INTO partition_expr_failure_t1 VALUES ('2024-09-12 00:30:00', 'a', 2)"
    String query = """SELECT sum(t1.num)
        FROM partition_expr_failure_t1 t1 INNER JOIN partition_expr_failure_t2 t2
            ON t1.data_date = t2.data_date
        WHERE to_date(t1.data_date) >= '2024-09-12'"""
    // MEMO PLAN evaluates the detailed failure supplier, which formats the partition expression.
    // Check that diagnosis completes, rather than only checking the fallback query result:
    // the rewrite rule catches exceptions, so the query can succeed even with the hashCode bug.
    explain {
        sql "MEMO PLAN ${query}"
        contains "need compensate union all, but can not, because the query structInfo"
        contains "mv partition info is org.apache.doris.mtmv.MTMVPartitionInfo@"
    }
    order_qt_result query
}
