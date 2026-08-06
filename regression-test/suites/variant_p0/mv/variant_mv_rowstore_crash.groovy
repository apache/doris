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

// Regression test for variant column with rowstore MV crash
// Bug: wrapp_array_nullable() updates data_types but not data_serdes,
// and ensure_root_node_type() updates data/data_types but not data_serdes,
// causing assert_cast failure during row store serialization in MV refresh.

suite("variant_mv_rowstore_crash", "variant_type") {

    def tbl = "var_mv_rs_tbl"
    def mv_name = "var_mv_rs_mv"

    sql "DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"
    sql "DROP TABLE IF EXISTS ${tbl}"

    // Test 1: MV with variant array extraction + rowstore
    sql """
        CREATE TABLE ${tbl} (
            k int,
            v variant NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true"
        );
    """

    // All rows have non-null array values in a single batch
    sql """INSERT INTO ${tbl} VALUES
        (1, '{"a":1,"arr":[{"x":1},{"x":2}]}'),
        (2, '{"a":2,"arr":[{"x":3}]}'),
        (3, '{"a":3,"arr":[{"x":4},{"x":5},{"x":6}]}')"""

    sql "DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"
    sql """
        CREATE MATERIALIZED VIEW ${mv_name}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true"
        )
        AS SELECT k, v['a'] AS a, v['arr'] AS arr FROM ${tbl};
    """

    String db = context.config.getDbNameByFile(context.file)
    def job_name = getJobName(db, mv_name)
    waitingMTMVTaskFinished(job_name)

    order_qt_mv "SELECT * FROM ${mv_name} ORDER BY k"

    // Test 2: INSERT INTO ... SELECT variant subcolumn into rowstore table
    // This ensures variant goes through parse_and_materialize_variant_columns
    // with ensure_root_node_type path
    def tbl3 = "var_rs_target_tbl"
    sql "DROP TABLE IF EXISTS ${tbl3}"
    sql """
        CREATE TABLE ${tbl3} (
            k int,
            arr variant NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true"
        );
    """
    // Insert variant subcolumn (array type) into rowstore table
    sql """INSERT INTO ${tbl3} SELECT k, v['arr'] FROM ${tbl}"""
    order_qt_direct "SELECT * FROM ${tbl3} ORDER BY k"

    sql "DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql "DROP TABLE IF EXISTS ${tbl3}"
}
