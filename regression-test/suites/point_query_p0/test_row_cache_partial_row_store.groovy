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

suite("test_row_cache_partial_row_store", "p0,nonConcurrent") {
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
        sql "SET enable_sql_cache=false"

        sql "DROP TABLE IF EXISTS row_cache_partial_row_store"
        sql "DROP TABLE IF EXISTS row_cache_full_row_store"
        sql """
            CREATE TABLE row_cache_partial_row_store (id BIGINT, v INT, c VARCHAR(64))
            UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("enable_unique_key_merge_on_write"="true", "replication_num"="1",
                        "row_store_columns"="id,v")
        """
        sql "INSERT INTO row_cache_partial_row_store VALUES (1, 100, 'old')"
        explain {
            sql "SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_partial_row_store WHERE id = 1"
            contains "SHORT-CIRCUIT"
        }
        assertEquals([[1L, 100]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_partial_row_store WHERE id = 1"))
        assertEquals([[1L, 100]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_partial_row_store WHERE id = 1"))
        sql "UPDATE row_cache_partial_row_store SET v = 200, c = 'new' WHERE id = 1"
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=false) */ id, v, c FROM row_cache_partial_row_store WHERE id = 1"))
        assertEquals([[1L, 200]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_partial_row_store WHERE id = 1"))
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v, c FROM row_cache_partial_row_store WHERE id = 1"))
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v, c FROM row_cache_partial_row_store WHERE id = 1"))

        // Full row stores remain eligible for the row cache and invalidate it on UPDATE.
        sql """
            CREATE TABLE row_cache_full_row_store (id BIGINT, v INT, c VARCHAR(64))
            UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("enable_unique_key_merge_on_write"="true", "replication_num"="1",
                        "store_row_column"="true")
        """
        sql "INSERT INTO row_cache_full_row_store VALUES (1, 100, 'old')"
        explain {
            sql "SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_full_row_store WHERE id = 1"
            contains "SHORT-CIRCUIT"
        }
        assertEquals([[1L, 100]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_full_row_store WHERE id = 1"))
        assertEquals([[1L, 100]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_full_row_store WHERE id = 1"))
        sql "UPDATE row_cache_full_row_store SET v = 200, c = 'new' WHERE id = 1"
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=false) */ id, v, c FROM row_cache_full_row_store WHERE id = 1"))
        assertEquals([[1L, 200]], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v FROM row_cache_full_row_store WHERE id = 1"))
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v, c FROM row_cache_full_row_store WHERE id = 1"))
        assertEquals([[1L, 200, 'new']], sql("SELECT /*+ SET_VAR(enable_short_circuit_query=true) */ id, v, c FROM row_cache_full_row_store WHERE id = 1"))
    } finally {
        set_be_config.call("disable_storage_row_cache", "true")
    }
}
