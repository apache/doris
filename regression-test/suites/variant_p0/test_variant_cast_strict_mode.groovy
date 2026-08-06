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

// Regression test for JIRA-233:
// INSERT INTO SELECT with LEFT JOIN on CTE that casts variant subcolumns
// returns all NULLs for the right-side CTE fields, while SELECT returns correct results.
//
// Root cause: In INSERT context, strict mode is enabled. When cast_from_variant_impl
// clones the FunctionContext, it inherits strict mode. The variant root column may have
// null/empty JSONB entries for rows where the subcolumn doesn't exist. In strict mode,
// these cause the entire cast to fail and return all NULLs.

suite("test_variant_cast_strict_mode", "variant_type") {

    sql """ set default_variant_enable_doc_mode = false """

    def variant_src = "test_variant_cast_strict_mode_src"
    def target_tbl = "test_variant_cast_strict_mode_target"

    sql "DROP TABLE IF EXISTS ${variant_src}"
    sql "DROP TABLE IF EXISTS ${target_tbl}"

    sql """
        CREATE TABLE ${variant_src} (
            `id` int NOT NULL,
            `content` variant NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert rows: some have "val" field, some don't (mixed schema)
    sql """
        INSERT INTO ${variant_src} VALUES
        (1, '{"val":"100.50", "name":"a"}'),
        (2, '{"val":"200.75", "name":"b"}'),
        (3, '{"name":"c"}'),
        (4, '{"val":"300.25", "name":"d"}')
    """

    sql """
        CREATE TABLE ${target_tbl} (
            `id` int NULL,
            `val_decimal` decimal(20,2) NULL,
            `name_str` varchar(100) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Step 1: Verify SELECT with variant cast returns correct results
    qt_select """
        WITH cte AS (
            SELECT
                id,
                cast(content['val'] AS decimal(20,2)) AS val_decimal,
                cast(content['name'] AS varchar) AS name_str
            FROM ${variant_src}
        )
        SELECT * FROM cte ORDER BY id
    """

    // Step 2: INSERT INTO SELECT with LEFT JOIN pattern (mirrors the original issue)
    // The LEFT JOIN pattern is key: v1 LEFT JOIN v3
    // v3 contains the variant->decimal cast
    sql """
        INSERT INTO ${target_tbl}
        WITH
            v1 AS (
                SELECT id FROM ${variant_src}
            ),
            v3 AS (
                SELECT
                    id,
                    cast(content['val'] AS decimal(20,2)) AS val_decimal,
                    cast(content['name'] AS varchar) AS name_str
                FROM ${variant_src}
            )
        SELECT
            v1.id,
            v3.val_decimal,
            v3.name_str
        FROM v1
        LEFT JOIN v3 ON v3.id = v1.id
    """

    // Step 3: Verify inserted results match SELECT results
    // Before fix: val_decimal and name_str were all NULL
    // After fix: should match the SELECT results
    qt_insert """
        SELECT * FROM ${target_tbl} ORDER BY id
    """

    // Step 4: Test direct INSERT INTO SELECT without LEFT JOIN (simpler case)
    sql "TRUNCATE TABLE ${target_tbl}"
    sql """
        INSERT INTO ${target_tbl}
        SELECT
            id,
            cast(content['val'] AS decimal(20,2)) AS val_decimal,
            cast(content['name'] AS varchar) AS name_str
        FROM ${variant_src}
    """
    qt_direct_insert """
        SELECT * FROM ${target_tbl} ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${variant_src}"
    sql "DROP TABLE IF EXISTS ${target_tbl}"
}
