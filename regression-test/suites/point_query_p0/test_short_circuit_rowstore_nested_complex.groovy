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

suite("test_short_circuit_rowstore_nested_complex", "p0,nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    def set_be_config = { key, value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id),
                    backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    try {
        set_be_config.call("disable_storage_row_cache", "false")

        sql "SET enable_nereids_planner=true"
        sql "SET enable_sql_cache=false"
        sql "SET enable_snapshot_point_query=true"
        sql "SET enable_short_circuit_query_access_column_store=true"

        sql "DROP TABLE IF EXISTS short_circuit_rowstore_nested_complex"
        sql """
            CREATE TABLE short_circuit_rowstore_nested_complex (
                pk INT,
                deep STRUCT<
                    nested_str: VARCHAR(64),
                    inner_s: STRUCT<deep_str: VARCHAR(64), flag: BOOLEAN, deep_char: CHAR(8)>,
                    deep_arr: ARRAY<STRUCT<verified: BOOLEAN, txt: VARCHAR(64), char_tag: CHAR(8)>>,
                    deep_map: MAP<VARCHAR(32), STRUCT<leaf: VARCHAR(64), n: INT, char_leaf: CHAR(8)>>
                > NULL,
                s STRUCT<str: VARCHAR(64), char_leaf: CHAR(8), num: INT, sibling: VARCHAR(64)> NULL
            ) ENGINE = OLAP
            UNIQUE KEY(pk)
            DISTRIBUTED BY HASH(pk) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true",
                "row_store_page_size" = "16384"
            )
        """

        sql """
            INSERT INTO short_circuit_rowstore_nested_complex VALUES
                (5,
                 named_struct(
                     'nested_str', 'b-deep-5',
                     'inner_s', named_struct('deep_str', 'b-inner-5', 'flag', true, 'deep_char', 'bd5'),
                     'deep_arr', array(named_struct('verified', false, 'txt', 'b-deep-arr-5', 'char_tag', 'bt5')),
                     'deep_map', map('b_key', named_struct('leaf', 'b-leaf-5', 'n', -5, 'char_leaf', 'bl5'))),
                 named_struct('str', 'b-s-5', 'char_leaf', 'bc5', 'num', -35, 'sibling', 'b-sib-5'))
        """
        sql "SYNC"

        sql "SET enable_short_circuit_query=false"
        order_qt_normal_path """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                   pk,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf')) AS hit_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf'))) AS hit_lower_len,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf')) AS miss_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf'))) AS miss_lower_len,
                   LENGTH(struct_element(struct_element(deep, 'inner_s'), 'deep_char')) AS char_storage_len,
                   ((struct_element(s, 'num') + 1) IS NULL) AS expr_is_null
            FROM short_circuit_rowstore_nested_complex
            WHERE pk = 5
        """

        sql "SET enable_short_circuit_query=true"
        explain {
            sql """
                SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                       pk,
                       CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf')) AS hit_char_len,
                       LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf'))) AS hit_lower_len,
                       CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf')) AS miss_char_len,
                       LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf'))) AS miss_lower_len,
                       LENGTH(struct_element(struct_element(deep, 'inner_s'), 'deep_char')) AS char_storage_len,
                       ((struct_element(s, 'num') + 1) IS NULL) AS expr_is_null
                FROM short_circuit_rowstore_nested_complex
                WHERE pk = 5
            """
            contains "SHORT-CIRCUIT"
        }
        order_qt_short_circuit_path """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                   pk,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf')) AS hit_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf'))) AS hit_lower_len,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf')) AS miss_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf'))) AS miss_lower_len,
                   LENGTH(struct_element(struct_element(deep, 'inner_s'), 'deep_char')) AS char_storage_len,
                   ((struct_element(s, 'num') + 1) IS NULL) AS expr_is_null
            FROM short_circuit_rowstore_nested_complex
            WHERE pk = 5
        """

        order_qt_short_circuit_path_repeat """
            SELECT /*+ SET_VAR(enable_nereids_planner=true) */
                   pk,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf')) AS hit_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'b_key'), 'leaf'))) AS hit_lower_len,
                   CHAR_LENGTH(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf')) AS miss_char_len,
                   LENGTH(LOWER(struct_element(element_at(struct_element(deep, 'deep_map'), 'dense'), 'leaf'))) AS miss_lower_len,
                   LENGTH(struct_element(struct_element(deep, 'inner_s'), 'deep_char')) AS char_storage_len,
                   ((struct_element(s, 'num') + 1) IS NULL) AS expr_is_null
            FROM short_circuit_rowstore_nested_complex
            WHERE pk = 5
        """
    } finally {
        set_be_config.call("disable_storage_row_cache", "true")
    }
}
