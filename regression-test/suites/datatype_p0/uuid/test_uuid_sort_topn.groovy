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

// Checklist: A05 D06 D08 E08.
suite("test_uuid_sort_topn", "p0") {
    sql "DROP TABLE IF EXISTS uuid_duplicate_sort_topn"
    sql """
        CREATE TABLE uuid_duplicate_sort_topn (
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
        INSERT INTO uuid_duplicate_sort_topn(u_key, seq, u_value) VALUES
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

    order_qt_uuid_topn """
        SELECT seq, CAST(u_key AS STRING) FROM uuid_duplicate_sort_topn ORDER BY u_key DESC LIMIT 3
    """
    explain {
        sql """
            verbose SELECT seq, CAST(u_key AS STRING)
            FROM uuid_duplicate_sort_topn ORDER BY u_key DESC LIMIT 3
        """
        contains "VTOP-N"
        contains "algorithm: heap sort"
    }
    sql "DROP TABLE IF EXISTS uuid_query_paths_sort_topn"
    sql """
        CREATE TABLE uuid_query_paths_sort_topn (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_sort_topn VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    order_qt_topn "SELECT id, u FROM uuid_query_paths_sort_topn ORDER BY u DESC NULLS LAST, id LIMIT 5"
}
