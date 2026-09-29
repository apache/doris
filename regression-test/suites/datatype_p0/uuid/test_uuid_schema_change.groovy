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

// Checklist: B05 F17.
suite("test_uuid_schema_change", "p0") {
    // Adding a UUID column validates schema-change serialization and default backfill.
    sql "DROP TABLE IF EXISTS uuid_schema_change"
    sql """
        CREATE TABLE uuid_schema_change (
            id INT,
            value STRING
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO uuid_schema_change VALUES (1, 'before'), (2, 'before'), (3, 'before')"
    sql """
        ALTER TABLE uuid_schema_change ADD COLUMN u UUID NOT NULL
        DEFAULT '550e8400-e29b-41d4-a716-446655440000'
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='uuid_schema_change'
                 ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    for (String functionName : ["UUID_V4", "UUID_V7", "GENERATE_UUID_V4", "GENERATE_UUID_V7"]) {
        test {
            sql "ALTER TABLE uuid_schema_change ADD COLUMN generated_uuid UUID NOT NULL DEFAULT ${functionName}()"
            exception "ADD COLUMN does not support UUID dynamic default values"
        }
        test {
            sql "ALTER TABLE uuid_schema_change ADD COLUMN (generated_uuid UUID NOT NULL DEFAULT ${functionName}())"
            exception "ADD COLUMN does not support UUID dynamic default values"
        }
    }
    sql "INSERT INTO uuid_schema_change(id, value) VALUES (4, 'after')"
    order_qt_uuid_schema_change """
        SELECT id, value, CAST(u AS STRING)
        FROM uuid_schema_change ORDER BY id
    """
    sql "DROP TABLE IF EXISTS uuid_storage_schema"
    sql """
        CREATE TABLE uuid_storage_schema (id INT, u UUID)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "light_schema_change"="false")
    """
    sql "INSERT INTO uuid_storage_schema VALUES (1, '80000000-0000-0000-0000-000000000000'), (2, NULL)"
    sql """ALTER TABLE uuid_storage_schema ADD COLUMN added UUID NOT NULL
           DEFAULT '00112233-4455-6677-8899-aabbccddeeff'"""
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='uuid_storage_schema' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    sql "INSERT INTO uuid_storage_schema(id,u) VALUES (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff')"
    order_qt_schema_backfill "SELECT * FROM uuid_storage_schema ORDER BY id"
    sql "ALTER TABLE uuid_storage_schema DROP COLUMN added"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='uuid_storage_schema' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    order_qt_schema_drop "SELECT * FROM uuid_storage_schema ORDER BY id"

}
