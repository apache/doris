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

// A short-circuit point query reading a column that a light schema change just added, before
// any load has carried the new schema to the BE.
//
// BaseTablet::_max_version_schema only moves forward when a rowset is written, and the lookup
// request carries no schema of its own, so in that window the BE's tablet schema still lacks
// the column. The column store fallback used to fail the query with
//   "field name is invalid. field=<col>"
// instead of answering with the column's default, which is what every segment holds for it.
//
// The fallback is only reached when the row store does not cover the column, i.e. with a
// partial "row_store_columns". A full row store answers from jsonb and never gets here.
suite("test_point_query_light_schema_change", "p0") {
    def tableName = "point_query_lsc_added_column"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `k` INT NOT NULL,
            `v1` INT,
            `v2` INT
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "row_store_columns" = "k,v1"
        )
    """
    sql """ INSERT INTO ${tableName} VALUES (1, 100, 1000), (2, 200, 2000) """

    sql "set enable_short_circuit_query = true"
    sql "set enable_short_circuit_query_access_column_store = true"

    // v2 is outside row_store_columns, so this already goes through the column store fallback.
    qt_before_alter_column_store """ SELECT k, v2 FROM ${tableName} WHERE k = 1 """

    def schemaChangeStatus = """ SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 """
    sql """ ALTER TABLE ${tableName} ADD COLUMN v_default INT DEFAULT "777" """
    waitForSchemaChangeDone({
        sql schemaChangeStatus
        time 600
    })
    sql """ ALTER TABLE ${tableName} ADD COLUMN v_null INT """
    waitForSchemaChangeDone({
        sql schemaChangeStatus
        time 600
    })

    // No load since the alter: the BE's tablet schema has neither column. Both must read as
    // their default rather than failing the query.
    qt_added_column_default """ SELECT k, v_default FROM ${tableName} WHERE k = 1 """
    qt_added_column_null """ SELECT k, v_null FROM ${tableName} WHERE k = 1 """
    qt_added_columns_together """ SELECT k, v2, v_default, v_null FROM ${tableName} WHERE k = 2 """

    // A load carries the new schema to the BE. Rows written before the alter still answer with
    // the default, now through the segment's own default iterator; the new row keeps its value.
    sql """ INSERT INTO ${tableName} VALUES (3, 300, 3000, 3777, 42) """
    qt_after_load_old_row """ SELECT k, v_default, v_null FROM ${tableName} WHERE k = 1 """
    qt_after_load_new_row """ SELECT k, v_default, v_null FROM ${tableName} WHERE k = 3 """
}
