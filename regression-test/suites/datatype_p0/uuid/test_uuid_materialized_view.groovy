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

// Checklist: B11 C11.
suite("test_uuid_materialized_view", "p0") {
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS uuid_duplicate_materialized_view"
    sql """
        CREATE TABLE uuid_duplicate_materialized_view (
            u_key UUID NOT NULL,
            seq INT NOT NULL,
            u_value UUID NULL DEFAULT NULL,
            u_default UUID NOT NULL DEFAULT "00000000-0000-0000-0000-000000000000"
        ) DUPLICATE KEY(u_key, seq)
        DISTRIBUTED BY HASH(u_key) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO uuid_duplicate_materialized_view(u_key, seq, u_value) VALUES
            ('00000000-0000-0000-0000-000000000001', 1, NULL),
            ('00000000-0000-0000-0000-000000000002', 2,
                '550e8400-e29b-41d4-a716-446655440000'),
            ('7fffffff-ffff-ffff-ffff-ffffffffffff', 3,
                '00000000-0000-0000-0000-000000000001'),
            ('80000000-0000-0000-0000-000000000000', 4,
                'ffffffff-ffff-ffff-ffff-ffffffffffff'),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff', 5,
                '550e8400-e29b-41d4-a716-446655440000')
    """
    sql "SYNC"

    sql "DROP TABLE IF EXISTS uuid_unique_mow_materialized_view"
    sql """
        CREATE TABLE uuid_unique_mow_materialized_view (
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
    sql "ALTER TABLE uuid_unique_mow_materialized_view ADD CONSTRAINT uuid_declared_pk PRIMARY KEY (u)"
    sql """
        INSERT INTO uuid_unique_mow_materialized_view VALUES
            ('00000000-0000-0000-0000-000000000001', 1,
                '00000000-0000-0000-0000-000000000001'),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff', 1,
                'ffffffff-ffff-ffff-ffff-ffffffffffff')
    """
    sql """
        INSERT INTO uuid_unique_mow_materialized_view VALUES
            ('00000000-0000-0000-0000-000000000001', 2,
                '550e8400-e29b-41d4-a716-446655440000')
    """
    order_qt_uuid_unique_mow """
        SELECT CAST(u AS STRING), version, CAST(payload AS STRING)
        FROM uuid_unique_mow_materialized_view
        WHERE u >= CAST('00000000-0000-0000-0000-000000000001' AS UUID)
        ORDER BY u
    """
    explain {
        sql """
            verbose SELECT version FROM uuid_unique_mow_materialized_view
            WHERE u = CAST(CONCAT('00000000-0000-0000-', '0000-000000000001') AS UUID)
        """
        contains "PREDICATES: ((u"
        contains "tablets=1/2"
    }

    // A synchronous single-table MV keeps UUID as a key, while an asynchronous
    // multi-table MV materializes a UUID join result as a natively typed column.
    sql "DROP MATERIALIZED VIEW IF EXISTS uuid_single_mv ON uuid_duplicate_materialized_view"
    createMV """
        CREATE MATERIALIZED VIEW uuid_single_mv AS
        SELECT u_key AS mv_u, seq AS mv_seq
        FROM uuid_duplicate_materialized_view ORDER BY mv_u, mv_seq
    """
    explain {
        sql "SELECT u_key, seq FROM uuid_duplicate_materialized_view ORDER BY u_key, seq"
        contains "uuid_duplicate_materialized_view(uuid_single_mv)"
    }
    order_qt_uuid_single_table_mv """
        SELECT CAST(u_key AS STRING), seq FROM uuid_duplicate_materialized_view ORDER BY u_key, seq
    """

    sql "DROP MATERIALIZED VIEW IF EXISTS uuid_multi_mtmv"
    sql """
        CREATE MATERIALIZED VIEW uuid_multi_mtmv
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY HASH(u) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT d.u_key AS u, d.seq, m.version
        FROM uuid_duplicate_materialized_view d JOIN uuid_unique_mow_materialized_view m ON d.u_key = m.u
    """
    waitingMTMVTaskFinishedByMvName("uuid_multi_mtmv")
    order_qt_uuid_multi_table_mv """
        SELECT CAST(u AS STRING), seq, version
        FROM uuid_multi_mtmv ORDER BY u, seq, version
    """
}
