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

suite("test_iceberg_mtmv_partition_scan_cost", "p0,external,iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    sql "SWITCH internal"
    sql "CREATE DATABASE IF NOT EXISTS regression_test_iceberg_mv_scan_cost"
    sql "USE regression_test_iceberg_mv_scan_cost"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_partition_scan_cost"
    sql "DROP CATALOG IF EXISTS iceberg_mv_scan_cost"
    sql """CREATE CATALOG iceberg_mv_scan_cost PROPERTIES (
        'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${restPort}',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password')"""
    sql "CREATE DATABASE IF NOT EXISTS iceberg_mv_scan_cost.mv_scan_cost"
    sql "DROP TABLE IF EXISTS iceberg_mv_scan_cost.mv_scan_cost.events"
    sql """CREATE TABLE iceberg_mv_scan_cost.mv_scan_cost.events (
        event_time DATETIMEV2(6), id INT, value INT)
        ENGINE=iceberg PARTITION BY LIST (DAY(event_time)) ()"""
    sql """INSERT INTO iceberg_mv_scan_cost.mv_scan_cost.events VALUES
        ('2024-01-01 00:00:00',1,10), ('2024-01-01 12:00:00',2,NULL),
        ('2024-01-02 00:00:00',3,30)"""
    sql """CREATE MATERIALIZED VIEW mv_partition_scan_cost
        BUILD DEFERRED REFRESH AUTO ON MANUAL PARTITION BY(event_time)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')
        AS SELECT event_time,id,value FROM iceberg_mv_scan_cost.mv_scan_cost.events"""
    sql "REFRESH MATERIALIZED VIEW mv_partition_scan_cost COMPLETE"
    waitingMTMVTaskFinishedByMvName("mv_partition_scan_cost", "regression_test_iceberg_mv_scan_cost")
    order_qt_materialized_rows "SELECT id,value FROM mv_partition_scan_cost"

    // Leave one new day outside the refreshed MV to exercise compensation candidates.
    sql """INSERT INTO iceberg_mv_scan_cost.mv_scan_cost.events VALUES
        ('2024-01-03 00:00:00',4,40), ('2024-01-03 01:00:00',5,NULL)"""
    sql "REFRESH TABLE iceberg_mv_scan_cost.mv_scan_cost.events"
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"

    // Check results without depending on cost values, row-count reports, or the winning plan.
    [false, true].each { rewrite ->
        sql "SET enable_materialized_view_rewrite = ${rewrite}"
        String suffix = rewrite ? "rewritten" : "reference"
        "order_qt_all_${suffix}" "SELECT id,value FROM iceberg_mv_scan_cost.mv_scan_cost.events"
        "order_qt_mixed_${suffix}" """SELECT id,value FROM iceberg_mv_scan_cost.mv_scan_cost.events
            WHERE event_time >= '2024-01-02' AND event_time < '2024-01-04'"""
        "order_qt_residual_${suffix}" """SELECT id,value FROM iceberg_mv_scan_cost.mv_scan_cost.events
            WHERE event_time >= '2024-01-03 00:30:00' AND event_time < '2024-01-03 02:00:00'
                AND value IS NULL"""
        "order_qt_nonpartition_${suffix}" """SELECT id,value FROM iceberg_mv_scan_cost.mv_scan_cost.events
            WHERE value IS NULL"""
        "qt_empty_aggregate_${suffix}" """SELECT count(*),sum(value)
            FROM iceberg_mv_scan_cost.mv_scan_cost.events WHERE event_time >= '2025-01-01'"""
    }
}
