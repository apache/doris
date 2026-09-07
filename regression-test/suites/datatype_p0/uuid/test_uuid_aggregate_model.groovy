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

// Checklist: B06 F12.
suite("test_uuid_aggregate_model", "p0") {
    // Aggregate model UUID key/value columns.
    sql "DROP TABLE IF EXISTS uuid_aggregate"
    sql """
        CREATE TABLE uuid_aggregate (
            u_key UUID NOT NULL,
            u_value UUID REPLACE,
            total BIGINT SUM
        ) AGGREGATE KEY(u_key)
        DISTRIBUTED BY HASH(u_key) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO uuid_aggregate VALUES
            ('00000000-0000-0000-0000-000000000001',
                '00000000-0000-0000-0000-000000000002', 10),
            ('00000000-0000-0000-0000-000000000001',
                '550e8400-e29b-41d4-a716-446655440000', 20)
    """
    order_qt_uuid_aggregate_model """
        SELECT CAST(u_key AS STRING), CAST(u_value AS STRING), total
        FROM uuid_aggregate ORDER BY u_key
    """
    sql "DROP TABLE IF EXISTS uuid_storage_aggregate"
    sql """
        CREATE TABLE uuid_storage_aggregate (
            id INT, lo UUID MIN, hi UUID MAX, kept UUID REPLACE_IF_NOT_NULL
        ) AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")
    """
    for (String value : ["00112233-4455-6677-8899-aabbccddeeff", "80000000-0000-0000-0000-000000000000",
                        "ffffffff-ffff-ffff-ffff-ffffffffffff"]) {
        sql "INSERT INTO uuid_storage_aggregate VALUES (1, '${value}', '${value}', '${value}'), (2, NULL, NULL, NULL)"
    }
    sql "INSERT INTO uuid_storage_aggregate VALUES (1, NULL, NULL, NULL)"
    order_qt_aggregate_before "SELECT * FROM uuid_storage_aggregate ORDER BY id"
    if (!isCloudMode()) {
        trigger_and_wait_compaction("uuid_storage_aggregate", "full")
    }
    order_qt_aggregate_after "SELECT * FROM uuid_storage_aggregate ORDER BY id"

}
