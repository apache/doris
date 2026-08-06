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

// Regression test for crash in DecimalComparison due to column/type
// nullability mismatch when runtime filter pushes MIN/MAX predicates
// involving decimal type casts on nullable columns.
suite("runtime_filter_cast") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_join_reorder=true"
    sql "set runtime_filter_type=4"
    sql "set runtime_filter_mode=GLOBAL"
    sql "set runtime_filter_wait_time_ms=10000"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set enable_runtime_filter_prune=false"
    sql "set expand_runtime_filter_by_inner_join=true"

    // Build-side table: small dimension table with decimal column
    sql "drop table if exists decimal_rf_build"
    sql """
        CREATE TABLE decimal_rf_build (
            `id` INT NOT NULL,
            `val_int_not_null` INT NOT NULL,
            `val` DECIMALV3(38, 10) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    // Probe-side table: fact table with nullable decimal of different precision
    // The different precision forces VCastExpr in the runtime filter comparison
    sql "drop table if exists decimal_rf_probe"
    sql """
        CREATE TABLE decimal_rf_probe (
            `id` INT NOT NULL,
            `val` DECIMALV3(15, 2) NOT NULL,
            `data` VARCHAR(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO decimal_rf_build VALUES
        (1, 100, 100.5000000000),
        (2, 200, 200.7500000000),
        (3, 300, 150.2500000000)
    """

    sql """
        INSERT INTO decimal_rf_probe VALUES
        (1, 50.12, 'a'),
        (2, 100.50, 'b'),
        (3, 150.25, 'c'),
        (4, 175.99, 'd'),
        (5, 200.75, 'e'),
        (7, 300.00, 'g')
    """

    sql "drop table if exists decimal_rf_probe_nullable"
    sql """
        CREATE TABLE decimal_rf_probe_nullable (
            `id` INT NOT NULL,
            `val` DECIMALV3(15, 2) NULL,
            `data` VARCHAR(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO decimal_rf_probe_nullable VALUES
        (12, 100.50, 'bb'),
        (13, 150.25, 'cc'),
        (15, 200.75, 'ee'),
        (17, 300.00, 'gg'),
        (99, NULL, 'null')
    """

    sql "SET @v0 = 1 ;"

    order_qt_decimal_rf_join """
        SELECT p.val, b.val_int_not_null
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON round(p.val, 1) = b.val_int_not_null
    """
    order_qt_decimal_rf_join_var """
        SELECT p.val, b.val_int_not_null
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON round(p.val, @v0) = b.val_int_not_null
    """

    order_qt_decimal_rf_except """
    select t1.val_int_not_null
    from decimal_rf_build t1
    except
    select round(t2.val, 1)
    from decimal_rf_probe t2
    """

    order_qt_decimal_rf_except_var """
    select t1.val_int_not_null
    from decimal_rf_build t1
    except
    select round(t2.val, @v0)
    from decimal_rf_probe t2
    """

    // Test 1: equi-join triggers runtime filter on probe decimal column
    // The different decimal precision/scale triggers VCastExpr in the
    // runtime filter's comparison, which previously crashed due to
    // nullable column / non-nullable type mismatch.
    order_qt_decimal_rf_eq """
        SELECT p.id, p.val, p.data, b.id as bid, b.val as bval
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON CAST(p.val AS DECIMALV3(38, 10)) = b.val
    """

    // Test 2: range join with <= comparison (matches crash stack trace)
    order_qt_decimal_rf_le """
        SELECT p.id, p.val, b.val as bval
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON CAST(p.val AS DECIMALV3(38, 10)) <= b.val
        WHERE b.id = 2
    """

    // Test 3: range join with >= comparison
    order_qt_decimal_rf_ge """
        SELECT p.id, p.val, b.val as bval
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON CAST(p.val AS DECIMALV3(38, 10)) >= b.val
        WHERE b.id = 1
    """

    // Test 4: implicit cast via different precision in join condition
    // (no explicit CAST, but FE inserts one due to precision mismatch)
    order_qt_decimal_rf_implicit_cast """
        SELECT p.id, p.val, b.val as bval
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_build b
        ON p.val = b.val
    """

    order_qt_decimal_rf_nullable_single_target """
        SELECT n.id, b.id as bid, n.val, b.val_int_not_null
        FROM decimal_rf_probe_nullable n
        INNER JOIN decimal_rf_build b
        ON round(n.val, 1) = b.val_int_not_null
    """

    // The top join's RF on p.val expands through the lower inner join to n.val.
    // This creates grouped legacy RF targets and covers the same nullable cast
    // calculation as the single-target path.
    order_qt_decimal_rf_grouped_targets """
        SELECT /*+ leading(p n b) */ p.id, n.id, b.id as bid, p.val, n.val
        FROM decimal_rf_probe p
        INNER JOIN decimal_rf_probe_nullable n ON p.val = n.val
        INNER JOIN decimal_rf_build b ON p.val = b.val
    """
}
