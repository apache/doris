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

// Test suite for Table Stream / Row Binlog exception (rejection) paths.
// Each sub-case asserts a DDL/DML/INCR query is rejected with the exact error
// message produced by the corresponding code path. These are boundary cases:
// once a regression silently lets a rejected operation succeed, it almost
// always means a broken binlog schema or an unsupported model slips through,
// so the assertions are kept tight (substring match on the real error text).
//
// Covered categories:
//   1. create table with binlog<Row> on unsupported models / columns
//   2. create stream on unsupported base tables / configs
//   3. schema change on binlog<Row> tables (allowed ops keep working, rejected
//      ops fail cleanly)
//   4. stream / @incr query boundary (consume after drop, incr without binlog)
//
// Error texts are sourced from:
//   - InternalCatalog.createOlapTable (binlog model / create-time column checks)
//   - OlapTable.checkAsTableStreamBaseTable (stream base table check)
//   - AlterOperations.checkRowBinlogAllow (schema change rejection)
//   - SchemaChangeHandler (add VARIANT / AutoInc column check)
//   - BindRelation (INCR / VERSION AS OF query check)
suite("test_table_stream_exception", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_exception_db"
    sql "CREATE DATABASE test_table_stream_exception_db"
    sql "USE test_table_stream_exception_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    try {
        // ================================================================
        // 1. create table with binlog<Row> on unsupported table models.
        //    Only DUPLICATE and UNIQUE-MoW support binlog<Row>; MoR (UNIQUE
        //    non-MoW) and AGGREGATE must be rejected at CREATE TABLE time.
        //    Source: InternalCatalog.createOlapTable.
        // ================================================================

        // 1.1 UNIQUE table without MoW (i.e. MoR) + binlog<Row> -> reject.
        sql "DROP TABLE IF EXISTS tbl_mor_binlog FORCE"
        test {
            sql """
                CREATE TABLE tbl_mor_binlog (
                    k1 INT, v1 INT
                )
                UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "false",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW"
                )
            """
            exception "Only duplicate and mow table model support binlog<Row>"
        }

        // 1.2 AGGREGATE table + binlog<Row> -> reject.
        sql "DROP TABLE IF EXISTS tbl_agg_binlog FORCE"
        test {
            sql """
                CREATE TABLE tbl_agg_binlog (
                    k1 INT, v1 SUM INT
                )
                AGGREGATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW"
                )
            """
            exception "Only duplicate and mow table model support binlog<Row>"
        }

        // 1.3 DUPLICATE table with need_historical_value=true -> reject.
        //    before-image only makes sense on MoW.
        sql "DROP TABLE IF EXISTS tbl_dup_hist FORCE"
        test {
            sql """
                CREATE TABLE tbl_dup_hist (
                    k1 INT, v1 INT
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW",
                    "binlog.need_historical_value" = "true"
                )
            """
            exception "Duplicate table model don't support record historical value"
        }

        // ================================================================
        // 2. create table with binlog<Row> on unsupported column types.
        //    VARIANT and AUTO_INCREMENT are rejected during CREATE TABLE
        //    validation before the row binlog schema is generated.
        // ================================================================

        // 2.1 binlog<Row> + VARIANT column -> reject.
        sql "DROP TABLE IF EXISTS tbl_variant_binlog FORCE"
        test {
            sql """
                CREATE TABLE tbl_variant_binlog (
                    k1 INT,
                    v1 VARIANT
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW"
                )
            """
            exception "variant column can't be created on table with binlog<Row>"
        }

        // 2.2 binlog<Row> + AUTO_INCREMENT column -> reject.
        sql "DROP TABLE IF EXISTS tbl_autoinc_binlog FORCE"
        test {
            sql """
                CREATE TABLE tbl_autoinc_binlog (
                    k1 INT,
                    v1 BIGINT NOT NULL AUTO_INCREMENT
                )
                UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "binlog.enable" = "true",
                    "binlog.format" = "ROW"
                )
            """
            exception "auto-inc column can't be created on table with binlog<Row>"
        }

        // ================================================================
        // 3. create stream on unsupported base tables / configs.
        //    Source: OlapTable.checkAsTableStreamBaseTable,
        //    TableIf.checkAsTableStreamBaseTable (default), InternalCatalog.
        // ================================================================

        // 3.1 create stream on a base table WITHOUT binlog enabled -> reject.
        sql "DROP TABLE IF EXISTS base_no_binlog FORCE"
        sql """
            CREATE TABLE base_no_binlog (
                k1 INT, v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        test {
            sql """
                CREATE STREAM s_no_binlog ON TABLE base_no_binlog
                PROPERTIES ("type" = "min_delta")
            """
            exception "need to enable row binlog for table stream"
        }

        // 3.2 create MIN_DELTA stream on a MoW table that did NOT enable
        //     need_historical_value -> reject.
        sql "DROP TABLE IF EXISTS base_mow_no_hist FORCE"
        sql """
            CREATE TABLE base_mow_no_hist (
                k1 INT, v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "false"
            )
        """
        test {
            sql """
                CREATE STREAM s_min_delta_no_hist ON TABLE base_mow_no_hist
                PROPERTIES ("type" = "min_delta")
            """
            exception "MIN_DELTA table stream requires base mow table to enable binlog.need_historical_value=true"
        }

        // 3.3 create stream on a View -> reject. Views fall through to the
        //     default TableIf.checkAsTableStreamBaseTable which throws for any
        //     non-OlapTable base type.
        sql "DROP VIEW IF EXISTS v_base"
        sql "DROP TABLE IF EXISTS base_for_view FORCE"
        sql """
            CREATE TABLE base_for_view (
                k1 INT, v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "CREATE VIEW v_base AS SELECT k1, v1 FROM base_for_view"
        test {
            sql """
                CREATE STREAM s_on_view ON TABLE v_base
                PROPERTIES ("type" = "append_only")
            """
            exception "is not supported for create table stream"
        }

        // ================================================================
        // 4. schema change on binlog<Row> tables.
        //    Allowed ops (ADD/DROP light column, ADD/DROP rollup, rename,
        //    partition ops, modify distribution, modify comment) should keep
        //    working; rejected ops (MODIFY/RENAME/REORDER column, alter multi
        //    partition) must fail cleanly so no half-broken binlog schema is
        //    left behind.
        //    Source: AlterOperations.checkRowBinlogAllow; AlterOp.allowOpRowBinlog
        //    defaults to false and only whitelisted ops override to true.
        // ================================================================

        sql "DROP TABLE IF EXISTS base_sc FORCE"
        sql """
            CREATE TABLE base_sc (
                k1 INT, k2 INT, v1 INT, v2 VARCHAR(16)
            )
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """

        // 4.1 ADD COLUMN with VARIANT type on a binlog<Row> table -> reject.
        test {
            sql "ALTER TABLE base_sc ADD COLUMN v3 VARIANT"
            exception "does not support VARIANT column"
        }

        // 4.2 MODIFY COLUMN (change type) -> reject.
        //     AllowOpRowBinlog default false for ModifyColumnOp.
        test {
            sql "ALTER TABLE base_sc MODIFY COLUMN v2 VARCHAR(32)"
            exception "Not allowed to perform current operation on Table With binlog<row>"
        }

        // 4.3 RENAME COLUMN -> reject.
        //     RenameColumnOp does not override allowOpRowBinlog -> default false.
        test {
            sql "ALTER TABLE base_sc RENAME COLUMN v1 vv1"
            exception "Not allowed to perform current operation on Table With binlog<row>"
        }

        // 4.4 REORDER COLUMNS -> reject.
        //     ReorderColumnsOp does not override allowOpRowBinlog -> default false.
        test {
            sql "ALTER TABLE base_sc ORDER BY (v2, v1)"
            exception "Not allowed to perform current operation on Table With binlog<row>"
        }

        // 4.5 sanity: a whitelisted op (ADD COLUMN INT) must still succeed on
        //     the same table, proving the table isn't left in a bad state by
        //     the rejected ops above.
        sql "ALTER TABLE base_sc ADD COLUMN v4 INT"
        sql "sync"

        // ================================================================
        // 5. stream / @incr query boundary cases.
        // ================================================================

        // 5.1 @incr on a table without binlog<Row> -> reject.
        //     Source: BindRelation INCR query check.
        sql "DROP TABLE IF EXISTS base_incr_no_binlog FORCE"
        sql """
            CREATE TABLE base_incr_no_binlog (
                k1 INT, v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO base_incr_no_binlog VALUES (1, 10)"
        sql "sync"
        test {
            sql """
                SELECT k1, v1, __DORIS_BINLOG_OP__
                FROM base_incr_no_binlog@incr("incrementType" = "DETAIL")
            """
            exception "INCR query requires ROW binlog enabled on base table"
        }

        // 5.2 MIN_DELTA @incr on a MoW table without need_historical_value
        //     -> reject. Source: BindRelation MIN_DELTA check.
        //     (base_mow_no_hist from case 3.2 is reused here.)
        sql "INSERT INTO base_mow_no_hist VALUES (1, 10)"
        sql "sync"
        test {
            sql """
                SELECT k1, v1, __DORIS_BINLOG_OP__
                FROM base_mow_no_hist@incr("incrementType" = "MIN_DELTA")
            """
            exception "MIN_DELTA INCR query requires base table to enable binlog.need_historical_value=true"
        }
    } finally {
        // cleanup. ORDER MATTERS: drop streams/views before base tables.
        sql "DROP STREAM IF EXISTS s_no_binlog"
        sql "DROP STREAM IF EXISTS s_min_delta_no_hist"
        sql "DROP STREAM IF EXISTS s_on_view"
        sql "DROP VIEW IF EXISTS v_base"
        sql "DROP DATABASE IF EXISTS test_table_stream_exception_db"
    }
}
