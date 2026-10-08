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

// Checklist: A04 F13 F15 G12.
suite("test_uuid_partial_update", "p0") {
    for (boolean rowStore : [false, true]) {
        for (String mode : ["UPDATE_FIXED_COLUMNS", "UPDATE_FLEXIBLE_COLUMNS"]) {
            sql "DROP TABLE IF EXISTS uuid_partial_defaults"
            sql """
                CREATE TABLE uuid_partial_defaults (
                    id INT NOT NULL,
                    value INT,
                    u4 UUID NOT NULL DEFAULT UUID_V4(),
                    u7 UUID NULL DEFAULT UUID_V7()
                ) UNIQUE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "enable_unique_key_skip_bitmap_column" = "true",
                    "store_row_column" = "${rowStore}"
                )
            """
            sql "INSERT INTO uuid_partial_defaults(id, value) VALUES (1, 10), (2, 20)"
            sql "DROP TABLE IF EXISTS uuid_partial_snapshot"
            sql "CREATE TABLE uuid_partial_snapshot PROPERTIES('replication_num'='1') AS SELECT * FROM uuid_partial_defaults"
            streamLoad {
                table "uuid_partial_defaults"
                set "format", "json"
                set "read_json_by_line", "true"
                set "unique_key_update_mode", mode
                if (mode == "UPDATE_FIXED_COLUMNS") {
                    set "columns", "id,value"
                }
                set "partial_update_new_key_behavior", "append"
                set "strict_mode", "false"
                inputStream new ByteArrayInputStream(('{"id":1,"value":11}\n'
                        + '{"id":3,"value":30}\n'
                        + '{"id":4,"value":40}\n').getBytes("UTF-8"))
                time 10000
            }
            order_qt_partial_defaults """
                SELECT id, value, UUID_VERSION(u4), UUID_VERSION(u7)
                FROM uuid_partial_defaults ORDER BY id
            """
            qt_partial_distinct """
                SELECT COUNT(*), COUNT(DISTINCT u4), COUNT(DISTINCT u7)
                FROM uuid_partial_defaults
            """
            qt_partial_preserve """
                SELECT COUNT(*) FROM uuid_partial_defaults d JOIN uuid_partial_snapshot s
                ON d.id = s.id AND d.u4 = s.u4 AND d.u7 = s.u7
            """
        }
    }
}
