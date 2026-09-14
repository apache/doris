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

// CIR-21356: information_schema.columns returns columns attributed to the
// wrong table when a table disappears between the getTableNames and
// describeTables RPCs, because describeTables does not append a tables_offset
// for the missing table. FE.describeTables.skipTable simulates the missing
// table by name. Skipping either table must not affect the IN query result
// of the other table.
suite("test_information_schema_columns_missing_table", "nonConcurrent") {
    sql """drop database if exists test_information_schema_columns_missing_table"""
    sql """create database test_information_schema_columns_missing_table"""
    sql """
        CREATE TABLE test_information_schema_columns_missing_table.tbl_a (
            tbl_a_c0 INT, tbl_a_c1 INT, tbl_a_c2 INT, tbl_a_c3 INT
        ) DUPLICATE KEY(tbl_a_c0) DISTRIBUTED BY HASH(tbl_a_c0) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_information_schema_columns_missing_table.tbl_b (
            tbl_b_c0 INT, tbl_b_c1 INT, tbl_b_c2 INT
        ) DUPLICATE KEY(tbl_b_c0) DISTRIBUTED BY HASH(tbl_b_c0) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    order_qt_baseline """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'test_information_schema_columns_missing_table'
        ORDER BY table_name, column_name
    """

    try {
        // simulate tbl_a being dropped between getTableNames and describeTables
        GetDebugPoint().enableDebugPointForAllFEs('FE.describeTables.skipTable', [value: 'tbl_a'])
        order_qt_skip_tbl_a """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'test_information_schema_columns_missing_table'
              AND table_name IN ('tbl_b', 'awdadw')
            ORDER BY table_name, column_name
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('FE.describeTables.skipTable')
    }

    try {
        // simulate tbl_b being dropped between getTableNames and describeTables
        GetDebugPoint().enableDebugPointForAllFEs('FE.describeTables.skipTable', [value: 'tbl_b'])
        order_qt_skip_tbl_b """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'test_information_schema_columns_missing_table'
              AND table_name IN ('tbl_a', 'awdadw')
            ORDER BY table_name, column_name
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('FE.describeTables.skipTable')
    }
}
