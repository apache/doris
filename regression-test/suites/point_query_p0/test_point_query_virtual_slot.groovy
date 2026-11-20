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

suite("test_point_query_virtual_slot") {
    def tableName = "point_query_virtual_slot"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            key_col INT NOT NULL,
            value_col INT NOT NULL
        ) UNIQUE KEY(key_col)
        DISTRIBUTED BY HASH(key_col) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "store_row_column" = "true",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "INSERT INTO ${tableName} VALUES (601, 601)"
    sql "SET enable_nereids_planner = true"
    sql "SET experimental_enable_virtual_slot_for_cse = true"
    sql "SET enable_short_circuit_query = true"
    sql "SET enable_sql_cache = false"

    def query = """
        SELECT key_col,
               key_col + value_col AS computed_value,
               key_col + value_col AS computed_value_again
        FROM ${tableName}
        WHERE key_col = 601
    """

    explain {
        sql query
        contains "SHORT-CIRCUIT"
    }

    qt_point_query_virtual_slot query

    // Validate a derived column whose input is not included in the row-store
    // projection and therefore must still be available for expression evaluation.
    sql "SET enable_short_circuit_query_access_column_store = true"
    sql "DROP TABLE IF EXISTS point_query_virtual_slot_partial"
    sql """
        CREATE TABLE point_query_virtual_slot_partial (
            key_col INT NOT NULL,
            value_col INT NOT NULL
        ) UNIQUE KEY(key_col)
        DISTRIBUTED BY HASH(key_col) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "row_store_columns" = "key_col",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "INSERT INTO point_query_virtual_slot_partial VALUES (601, 601)"

    // Validate a nested derived expression and ensure that all expression
    // dependencies are resolved before the final value is produced. Run this
    // independently of the physical-column coverage below.
    def nestedQuery = """
        SELECT key_col,
               (key_col + value_col) + (key_col + value_col) AS nested_value,
               (key_col + value_col) + (key_col + value_col) AS nested_value_again
        FROM point_query_virtual_slot_partial
        WHERE key_col = 601
    """
    order_qt_point_query_nested_virtual_slot nestedQuery

    def partialQuery = """
        SELECT key_col,
               key_col + value_col AS computed_value,
               key_col + value_col AS computed_value_again
        FROM point_query_virtual_slot_partial
        WHERE key_col = 601
    """
    explain {
        sql partialQuery
        contains "SHORT-CIRCUIT"
    }
    order_qt_point_query_virtual_slot_partial partialQuery
}
