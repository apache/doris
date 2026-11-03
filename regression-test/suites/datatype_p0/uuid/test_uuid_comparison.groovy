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

// Checklist: A05 C07 E01 E03.
suite("test_uuid_comparison", "p0") {
    sql "DROP TABLE IF EXISTS uuid_duplicate_comparison"
    sql """
        CREATE TABLE uuid_duplicate_comparison (
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
        INSERT INTO uuid_duplicate_comparison(u_key, seq, u_value) VALUES
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

    order_qt_uuid_comparison_predicates """
        SELECT seq,
               u_key = CAST('00000000-0000-0000-0000-000000000002' AS UUID),
               u_key != CAST('00000000-0000-0000-0000-000000000002' AS UUID),
               u_key > CAST('00000000-0000-0000-0000-000000000002' AS UUID),
               u_key >= CAST('00000000-0000-0000-0000-000000000002' AS UUID),
               u_key < CAST('00000000-0000-0000-0000-000000000002' AS UUID),
               u_key <= CAST('00000000-0000-0000-0000-000000000002' AS UUID)
        FROM uuid_duplicate_comparison ORDER BY seq
    """
    order_qt_uuid_in_predicates """
        SELECT seq FROM uuid_duplicate_comparison
        WHERE u_key IN (
            CAST('00000000-0000-0000-0000-000000000001' AS UUID),
            CAST('80000000-0000-0000-0000-000000000000' AS UUID))
          AND u_key NOT IN (CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID))
        ORDER BY seq
    """
    order_qt_uuid_combined_predicates """
        SELECT seq FROM uuid_duplicate_comparison
        WHERE u_key >= CAST('80000000-0000-0000-0000-000000000000' AS UUID)
          AND u_value = CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID)
        ORDER BY seq
    """
}
