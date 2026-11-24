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

suite("test_ivm_sink_truncation_signature") {
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"
    sql "SET enable_insert_value_auto_cast = true"
    sql "SET enable_strict_cast = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_sink_truncation_mv"
    sql "DROP TABLE IF EXISTS ivm_sink_truncation_sales"
    sql "DROP TABLE IF EXISTS ivm_sink_truncation_refunds"

    sql """
        CREATE TABLE ivm_sink_truncation_sales (
            sale_id BIGINT NOT NULL,
            channel VARCHAR(16),
            amount DECIMAL(12,2)
        )
        UNIQUE KEY(sale_id)
        DISTRIBUTED BY HASH(sale_id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'changelog.enable' = 'true',
            'changelog.need_historical_value' = 'true'
        )
    """

    sql """
        CREATE TABLE ivm_sink_truncation_refunds (
            refund_id BIGINT NOT NULL,
            amount DECIMAL(12,2)
        )
        UNIQUE KEY(refund_id)
        DISTRIBUTED BY HASH(refund_id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'enable_unique_key_merge_on_write' = 'true',
            'changelog.enable' = 'true',
            'changelog.need_historical_value' = 'true'
        )
    """

    sql """
        INSERT INTO ivm_sink_truncation_sales VALUES
            (101, 'web', 120.00),
            (102, 'app', 80.00),
            (103, 'web', 200.00),
            (104, 'store', 50.00),
            (105, 'web', NULL)
    """
    sql "INSERT INTO ivm_sink_truncation_refunds VALUES (901, 10.00), (902, 20.00)"

    sql """
        CREATE MATERIALIZED VIEW ivm_sink_truncation_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT u.channel, COUNT(*) AS row_count, SUM(u.amount) AS total_amount
        FROM (
            SELECT channel, amount FROM ivm_sink_truncation_sales
            UNION ALL
            SELECT 'refund' AS channel, -amount AS amount FROM ivm_sink_truncation_refunds
        ) u
        GROUP BY u.channel
    """

    // Preview the first refresh, then let the actual write establish the signature.
    // The write plan still truncates the STRING union output to the target VARCHAR length.
    explain {
        sql "ANALYZED PLAN REFRESH MATERIALIZED VIEW ivm_sink_truncation_mv INCREMENTAL WITH ALL STREAMS"
        contains "LogicalOlapTableSink"
        contains "substring(channel#"
    }

    def refreshTaskQuery = """
        SELECT Status, IvmRefreshMethod
        FROM tasks('type' = 'mv')
        WHERE MvDatabaseName = '${context.dbName}' AND MvName = 'ivm_sink_truncation_mv'
        ORDER BY CreateTime DESC, CAST(TaskId AS BIGINT) DESC
        LIMIT 1
    """

    sql "REFRESH MATERIALIZED VIEW ivm_sink_truncation_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_sink_truncation_mv")
    qt_first_refresh_method refreshTaskQuery
    order_qt_after_first_incremental "SELECT channel, row_count, total_amount FROM ivm_sink_truncation_mv"

    sql "INSERT INTO ivm_sink_truncation_sales VALUES (106, 'web', 150.00)"
    sql "INSERT INTO ivm_sink_truncation_refunds VALUES (903, 30.00)"
    // A dry run has no table write projection, even after a write signature exists.
    ivm_hidden_qt("order_ivm_sink_truncation_dry_run", """
        REFRESH MATERIALIZED VIEW ivm_sink_truncation_mv INCREMENTAL WITH DRY RUN
    """)
    sql "REFRESH MATERIALIZED VIEW ivm_sink_truncation_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_sink_truncation_mv")
    qt_refresh_method refreshTaskQuery
    order_qt_after_incremental "SELECT channel, row_count, total_amount FROM ivm_sink_truncation_mv"
}
