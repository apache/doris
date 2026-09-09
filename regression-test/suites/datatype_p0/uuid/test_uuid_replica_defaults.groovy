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

suite("test_uuid_replica_defaults", "p0") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    if (isCloudMode()) {
        return
    }
    try {
        for (boolean memtableOnSink : [false, true]) {
            for (boolean rowStore : [false, true]) {
                for (String mode : ["UPDATE_FIXED_COLUMNS", "UPDATE_FLEXIBLE_COLUMNS"]) {
                    sql "DROP TABLE IF EXISTS uuid_replica_defaults"
                    sql """
                        CREATE TABLE uuid_replica_defaults (
                            id INT NOT NULL,
                            value INT,
                            u4 UUID NOT NULL DEFAULT UUID_V4(),
                            u7 UUID NULL DEFAULT UUID_V7()
                        ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES (
                            "replication_num" = "3",
                            "enable_unique_key_merge_on_write" = "true",
                            "enable_unique_key_skip_bitmap_column" = "true",
                            "store_row_column" = "${rowStore}"
                        )
                    """
                    streamLoad {
                        table "uuid_replica_defaults"
                        set "format", "json"
                        set "memtable_on_sink_node", "${memtableOnSink}"
                        set "read_json_by_line", "true"
                        set "unique_key_update_mode", mode
                        if (mode == "UPDATE_FIXED_COLUMNS") {
                            set "columns", "id,value"
                        }
                        set "partial_update_new_key_behavior", "append"
                        set "strict_mode", "false"
                        inputStream new ByteArrayInputStream((1..128).collect {
                            '{"id":' + it + ',"value":' + it + '}\n'
                        }.join().getBytes("UTF-8"))
                        time 10000
                    }
                    sql "SET use_fix_replica = 0"
                    sql "DROP TABLE IF EXISTS uuid_replica_snapshot"
                    sql "CREATE TABLE uuid_replica_snapshot PROPERTIES('replication_num'='1') AS SELECT * FROM uuid_replica_defaults"
                    for (int replica : [0, 1, 2]) {
                        sql "SET use_fix_replica = ${replica}"
                        qt_replica_distinct """
                            SELECT COUNT(*), COUNT(DISTINCT u4), COUNT(DISTINCT u7),
                                   MIN(UUID_VERSION(u4)), MIN(UUID_VERSION(u7))
                            FROM uuid_replica_defaults
                        """
                        qt_replica_equal """
                            SELECT COUNT(*) FROM uuid_replica_defaults d JOIN uuid_replica_snapshot s
                            ON d.id = s.id AND d.u4 = s.u4 AND d.u7 = s.u7
                        """
                    }
                    sql "SET use_fix_replica = -1"
                }
            }
        }
    } finally {
        sql "SET use_fix_replica = -1"
    }
}
