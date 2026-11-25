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

suite("test_table_delete_bitmap_lock_version", "p0") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS mow_lock_v2_default"
    sql "DROP TABLE IF EXISTS mow_lock_v1_explicit"
    sql """CREATE TABLE mow_lock_v2_default (k INT, v INT)
           UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""
    sql """CREATE TABLE mow_lock_v1_explicit (k INT, v INT)
           UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                       "delete_bitmap_lock_version" = "v1")"""

    // The rest of SHOW CREATE can vary with the environment, including the storage vault name.
    assertTrue(sql("SHOW CREATE TABLE mow_lock_v2_default")[0][1]
            .contains('"delete_bitmap_lock_version" = "v2"'))
    assertTrue(sql("SHOW CREATE TABLE mow_lock_v1_explicit")[0][1]
            .contains('"delete_bitmap_lock_version" = "v1"'))

    test {
        sql """ALTER TABLE mow_lock_v2_default SET ("delete_bitmap_lock_version" = "v1")"""
        exception "delete_bitmap_lock_version can only be specified when creating a table"
    }
    test {
        sql """CREATE TABLE mow_lock_bad_value (k INT, v INT)
               UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                           "delete_bitmap_lock_version" = "invalid")"""
        exception "delete_bitmap_lock_version must be v1 or v2"
    }
    test {
        sql """CREATE TABLE dup_lock_v2 (k INT, v INT)
               DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES ("replication_num" = "1", "delete_bitmap_lock_version" = "v2")"""
        exception "delete_bitmap_lock_version is only supported for cloud merge-on-write tables"
    }

    sql "INSERT INTO mow_lock_v2_default VALUES (1, 10), (2, 20)"
    sql "INSERT INTO mow_lock_v2_default VALUES (1, 11)"
    qt_v2 "SELECT k, v FROM mow_lock_v2_default ORDER BY k"
    sql "INSERT INTO mow_lock_v1_explicit VALUES (1, 10), (2, 20)"
    sql "INSERT INTO mow_lock_v1_explicit VALUES (1, 11)"
    qt_v1 "SELECT k, v FROM mow_lock_v1_explicit ORDER BY k"
}
