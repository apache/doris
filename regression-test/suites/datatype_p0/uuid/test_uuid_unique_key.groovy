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

// Checklist: B06 B07 F06 F13.
suite("test_uuid_unique_key", "p0") {
    sql "DROP TABLE IF EXISTS uuid_unique_mow_unique_key"
    sql """
        CREATE TABLE uuid_unique_mow_unique_key (
            u UUID NOT NULL,
            version INT,
            payload UUID
        ) UNIQUE KEY(u)
        DISTRIBUTED BY HASH(u) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "short_key" = "1"
        )
    """
    sql "ALTER TABLE uuid_unique_mow_unique_key ADD CONSTRAINT uuid_declared_pk PRIMARY KEY (u)"
    sql """
        INSERT INTO uuid_unique_mow_unique_key VALUES
            ('00000000-0000-0000-0000-000000000001', 1,
                '00000000-0000-0000-0000-000000000001'),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff', 1,
                'ffffffff-ffff-ffff-ffff-ffffffffffff')
    """
    sql """
        INSERT INTO uuid_unique_mow_unique_key VALUES
            ('00000000-0000-0000-0000-000000000001', 2,
                '550e8400-e29b-41d4-a716-446655440000')
    """
    order_qt_uuid_unique_mow """
        SELECT CAST(u AS STRING), version, CAST(payload AS STRING)
        FROM uuid_unique_mow_unique_key
        WHERE u >= CAST('00000000-0000-0000-0000-000000000001' AS UUID)
        ORDER BY u
    """
    explain {
        sql """
            verbose SELECT version FROM uuid_unique_mow_unique_key
            WHERE u = CAST(CONCAT('00000000-0000-0000-', '0000-000000000001') AS UUID)
        """
        contains "PREDICATES: ((u"
        contains "tablets=1/2"
    }
}
