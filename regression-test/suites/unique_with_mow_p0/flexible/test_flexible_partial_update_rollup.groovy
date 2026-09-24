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

suite("test_flexible_partial_update_rollup") {
    sql "DROP TABLE IF EXISTS test_flexible_partial_update_rollup"
    sql """
        CREATE TABLE test_flexible_partial_update_rollup (
            name VARCHAR(64) NOT NULL,
            region VARCHAR(16) NOT NULL,
            score INT NOT NULL DEFAULT '0',
            id BIGINT NOT NULL AUTO_INCREMENT
        ) UNIQUE KEY(name, region)
        DISTRIBUTED BY HASH(name) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "false"
        )
    """

    streamLoad {
        table "test_flexible_partial_update_rollup"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'strict_mode', 'false'
        inputStream new ByteArrayInputStream('{"name":"alpha","region":"r1","score":1}'.getBytes())
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    sql """
        ALTER TABLE test_flexible_partial_update_rollup
        ADD ROLLUP rollup_without_auto_inc(region, name, score)
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE ROLLUP WHERE TableName='test_flexible_partial_update_rollup'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }

    streamLoad {
        table "test_flexible_partial_update_rollup"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'strict_mode', 'false'
        inputStream new ByteArrayInputStream('{"name":"alpha","region":"r1","score":2}'.getBytes())
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Flexible partial update is not supported on tables with rollup"))
        }
    }

    // Full-row writes remain available after rejecting the flexible update.
    sql """INSERT INTO test_flexible_partial_update_rollup(name, region, score)
           VALUES ('beta', 'r2', 3)"""
}
