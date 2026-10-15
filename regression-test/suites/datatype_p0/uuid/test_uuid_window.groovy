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

// Checklist: C04 E09.
suite("test_uuid_window", "p0") {
    sql "DROP TABLE IF EXISTS uuid_query_paths_window"
    sql """
        CREATE TABLE uuid_query_paths_window (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_window VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    order_qt_window """
        SELECT id, u, ROW_NUMBER() OVER (ORDER BY u NULLS FIRST, id),
               LAG(u, 1, CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID))
                   OVER (PARTITION BY grp ORDER BY id),
               LEAD(u) OVER (PARTITION BY grp ORDER BY id),
               FIRST_VALUE(u) OVER (PARTITION BY grp ORDER BY id),
               LAST_VALUE(u) OVER (PARTITION BY grp ORDER BY id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
               MAX(u) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
        FROM uuid_query_paths_window ORDER BY id
    """
}
