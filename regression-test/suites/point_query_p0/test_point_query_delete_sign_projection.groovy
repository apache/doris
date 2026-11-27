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

suite("test_point_query_delete_sign_projection", "p0") {
    sql "DROP TABLE IF EXISTS point_query_delete_sign_projection"
    sql """
        CREATE TABLE point_query_delete_sign_projection (k INT, v INT, w INT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "row_store_columns" = "v"
        )
    """
    sql "INSERT INTO point_query_delete_sign_projection VALUES (1,100,1000),(2,NULL,2000),(3,-7,NULL)"
    sql "SET enable_short_circuit_query = true"
    sql "SET enable_short_circuit_query_access_column_store = true"
    sql "SET show_hidden_columns = true"
    try {
        // No hidden column is projected, so the FE still selects the point-query executor.
        // Debug visibility removes the delete filter and its tuple slot.
        explain {
            sql "SELECT v FROM point_query_delete_sign_projection WHERE k=1"
            contains "SHORT-CIRCUIT"
        }
        qt_debug_value "SELECT v FROM point_query_delete_sign_projection WHERE k=1"
        qt_debug_missing_columns "SELECT k,v,w FROM point_query_delete_sign_projection WHERE k=1"
        qt_debug_null "SELECT v FROM point_query_delete_sign_projection WHERE k=2"
        qt_debug_not_found "SELECT v FROM point_query_delete_sign_projection WHERE k=999"
        sql "SET enable_short_circuit_query_access_column_store = false"
        qt_debug_without_column_store "SELECT v FROM point_query_delete_sign_projection WHERE k=3"

        // Normal visibility must retain delete-sign reads even though the SELECT omits it.
        sql "SET show_hidden_columns = false"
        sql "SET enable_short_circuit_query_access_column_store = true"
        sql "DELETE FROM point_query_delete_sign_projection WHERE k=2"
        qt_normal_live "SELECT v FROM point_query_delete_sign_projection WHERE k=1"
        qt_normal_deleted "SELECT v FROM point_query_delete_sign_projection WHERE k=2"
    } finally {
        sql "SET show_hidden_columns = false"
        sql "SET enable_short_circuit_query_access_column_store = true"
    }
}
