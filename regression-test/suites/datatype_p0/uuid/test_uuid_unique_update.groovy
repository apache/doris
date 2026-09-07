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

// Checklist: F13 F14 F15 F16.
suite("test_uuid_unique_update", "p0") {
    for (boolean mow : [false, true]) {
        for (boolean rowStore : [false, true]) {
            sql "DROP TABLE IF EXISTS uuid_storage_unique"
            sql """
                CREATE TABLE uuid_storage_unique (u UUID NOT NULL, v UUID NULL, seq INT)
                UNIQUE KEY(u) DISTRIBUTED BY HASH(u) BUCKETS 1
                PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="${mow}",
                           "store_row_column"="${rowStore}", "function_column.sequence_col"="seq",
                           "disable_auto_compaction"="true")
            """
            for (int seq : [1, 3, 2]) {
                sql """INSERT INTO uuid_storage_unique VALUES
                    ('00112233-4455-6677-8899-aabbccddeeff',
                     '${seq == 3 ? "ffffffff-ffff-ffff-ffff-ffffffffffff" : "00000000-0000-0000-0000-000000000000"}', ${seq}),
                    ('80000000-0000-0000-0000-000000000000', NULL, ${seq})"""
            }
            order_qt_unique_before "SELECT * FROM uuid_storage_unique ORDER BY u"
            sql """UPDATE uuid_storage_unique SET v = '00112233-4455-6677-8899-aabbccddeeff', seq = 4
                   WHERE u = '80000000-0000-0000-0000-000000000000'"""
            if (!isCloudMode()) {
                trigger_and_wait_compaction("uuid_storage_unique", "full")
            }
            order_qt_unique_after "SELECT * FROM uuid_storage_unique ORDER BY u"
            qt_point "SELECT v, seq FROM uuid_storage_unique WHERE u = CAST('00112233445566778899AABBCCDDEEFF' AS UUID)"
            sql "DELETE FROM uuid_storage_unique WHERE u = '80000000-0000-0000-0000-000000000000'"
            order_qt_unique_deleted "SELECT * FROM uuid_storage_unique ORDER BY u"
        }
    }
}
