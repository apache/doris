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

suite("test_uuid_partial_update_schema", "p0") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    if (isCloudMode()) {
        return
    }
    try {
        for (boolean memtableOnSink : [false, true]) {
            for (boolean lightSchemaChange : [false, true]) {
                def modes = lightSchemaChange
                        ? ["UPDATE_FIXED_COLUMNS", "UPDATE_FLEXIBLE_COLUMNS"]
                        : ["UPDATE_FIXED_COLUMNS"]
                for (String mode : modes) {
                    // Flexible partial update requires a skip-bitmap column in every index;
                    // the existing ADD ROLLUP path does not propagate that hidden column.
                    boolean withRollup = lightSchemaChange && mode == "UPDATE_FIXED_COLUMNS"
                    sql "DROP TABLE IF EXISTS uuid_schema_defaults"
                    sql """
                        CREATE TABLE uuid_schema_defaults (
                            k1 INT NOT NULL, k2 INT NOT NULL, pad INT,
                            v4a UUID NOT NULL DEFAULT UUID_V4(),
                            v4b UUID NOT NULL DEFAULT UUID_V4(),
                            v7a UUID NULL DEFAULT UUID_V7(),
                            v7b UUID NULL DEFAULT UUID_V7()
                        ) UNIQUE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES (
                            "replication_num" = "3",
                            "enable_unique_key_merge_on_write" = "true",
                            "enable_unique_key_skip_bitmap_column" = "${lightSchemaChange}",
                            "light_schema_change" = "${lightSchemaChange}"
                        )
                    """
                    if (withRollup) {
                        // Change both key order and the UUID columns' index-local IDs.
                        sql """ALTER TABLE uuid_schema_defaults ADD ROLLUP
                               uuid_schema_rollup(k2, k1, v4b, v4a, v7b, v7a)"""
                        waitForSchemaChangeDone {
                            sql """SHOW ALTER TABLE ROLLUP WHERE TableName='uuid_schema_defaults'
                                   ORDER BY CreateTime DESC LIMIT 1"""
                            time 120
                        }
                    }
                    streamLoad {
                        table "uuid_schema_defaults"
                        set "format", "json"
                        set "read_json_by_line", "true"
                        set "memtable_on_sink_node", "${memtableOnSink}"
                        set "unique_key_update_mode", mode
                        if (mode == "UPDATE_FIXED_COLUMNS") {
                            set "columns", "k1,k2"
                        }
                        set "partial_update_new_key_behavior", "append"
                        set "strict_mode", "false"
                        inputStream new ByteArrayInputStream((1..128).collect {
                            '{"k1":' + it + ',"k2":' + (it + 1000) + '}\n'
                        }.join().getBytes("UTF-8"))
                        time 10000
                    }
                    for (int replica : [0, 1, 2]) {
                        sql "SET use_fix_replica = ${replica}"
                        qt_distinct_defaults """
                            SELECT COUNT(*), COUNT(DISTINCT v4a), COUNT(DISTINCT v4b),
                                   COUNT(DISTINCT v7a), COUNT(DISTINCT v7b),
                                   SUM(v4a <> v4b), SUM(v7a <> v7b)
                            FROM uuid_schema_defaults INDEX uuid_schema_defaults
                        """
                        if (withRollup) {
                            qt_rollup_equal """
                                WITH base_rows AS (
                                    SELECT k1, k2, v4a, v4b, v7a, v7b
                                    FROM uuid_schema_defaults INDEX uuid_schema_defaults
                                ), rollup_rows AS (
                                    SELECT k1, k2, v4a, v4b, v7a, v7b
                                    FROM uuid_schema_defaults INDEX uuid_schema_rollup
                                )
                                SELECT COUNT(*) FROM base_rows b JOIN rollup_rows r
                                ON b.k1 = r.k1 AND b.k2 = r.k2 AND b.v4a = r.v4a
                                   AND b.v4b = r.v4b AND b.v7a = r.v7a AND b.v7b = r.v7b
                            """
                        }
                    }
                    sql "SET use_fix_replica = -1"
                }
            }
        }
    } finally {
        sql "SET use_fix_replica = -1"
    }
}
