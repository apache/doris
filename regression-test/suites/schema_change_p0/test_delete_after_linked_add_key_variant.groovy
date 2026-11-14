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

// A DUPLICATE KEY table with a VARIANT column gets a new key column appended after the
// existing keys. The BE handles it as a linked schema change, so the new tablet's
// rowsets keep the old rowset schema (without the new key column). A DELETE that
// references the new key column is then pushed before any other load. The delete rowset
// must carry the FE schema version; otherwise full compaction picks the old rowset schema,
// treats the new key column as dropped and aborts the BE.
suite("test_delete_after_linked_add_key_variant", "p0") {
    def tableName = "test_delete_after_linked_add_key_variant"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            k3 INT NOT NULL,
            v1 VARIANT NOT NULL,
            v2 INT NULL
        )
        DUPLICATE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """ INSERT INTO ${tableName} VALUES (1, 1, 1, '{"a": 1}', 1), (2, 2, 2, '{"a": 2, "b": "x"}', 2) """
    sql """ INSERT INTO ${tableName} VALUES (3, 3, 3, '{"c": [1, 2]}', 3) """

    // Appended key column, short key count unchanged and no delete predicate yet:
    // the BE takes the linked schema change path.
    sql """ ALTER TABLE ${tableName} ADD COLUMN k4 DATE KEY """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    // No load between the schema change and the delete, so the tablet's max version schema
    // is still the one the new tablet was created with.
    sql """ DELETE FROM ${tableName} WHERE k1 = 1 AND k4 IS NULL """
    order_qt_before_compaction """ SELECT k1, k2, k3, k4, v2 FROM ${tableName} """

    trigger_and_wait_compaction(tableName, "full")

    order_qt_after_compaction """ SELECT k1, k2, k3, k4, v2 FROM ${tableName} """

    sql """ INSERT INTO ${tableName} VALUES (4, 4, 4, '2026-01-01', '{"a": 4}', 4) """
    order_qt_after_insert """ SELECT k1, k2, k3, k4, v2 FROM ${tableName} """

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
