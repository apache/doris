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

// Checklist: A08 H01 H02 H03 H04 H05 H07 H08.
suite("test_uuid_nested_types", "p0") {
    sql "DROP TABLE IF EXISTS uuid_query_paths_nested_types"
    sql """
        CREATE TABLE uuid_query_paths_nested_types (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_nested_types VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    sql "DROP TABLE IF EXISTS uuid_nested_paths_nested_types"
    sql """
        CREATE TABLE uuid_nested_paths_nested_types (
            id INT, a ARRAY<UUID>, m MAP<UUID, ARRAY<UUID>>,
            s STRUCT<k:UUID, a:ARRAY<UUID>>, aa ARRAY<ARRAY<UUID>>
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_nested_paths_nested_types
        SELECT id, ARRAY(u, NULL), MAP(u, ARRAY(NULL, u)),
               NAMED_STRUCT('k', u, 'a', ARRAY(u, NULL)), ARRAY(ARRAY(u, NULL), ARRAY(u))
        FROM uuid_query_paths_nested_types WHERE u IS NOT NULL
    """
    sql "INSERT INTO uuid_nested_paths_nested_types VALUES (9, NULL, NULL, NULL, NULL), (10, [], {}, NULL, [])"
    order_qt_nested_storage "SELECT * FROM uuid_nested_paths_nested_types ORDER BY id"
    order_qt_nested_projection """
        SELECT id, a[1], s.k, s.a[2], aa[1][1], MAP_KEYS(m), MAP_VALUES(m),
               ARRAY_MAP(x -> CAST(x AS STRING), a)
        FROM uuid_nested_paths_nested_types ORDER BY id
    """
    order_qt_explode """
        SELECT id, item FROM uuid_nested_paths_nested_types LATERAL VIEW EXPLODE_OUTER(a) t AS item
        ORDER BY id, item NULLS FIRST
    """
    order_qt_nested_union """
        SELECT * FROM (SELECT a FROM uuid_nested_paths_nested_types WHERE id < 4
                       UNION ALL SELECT CAST(NULL AS ARRAY<UUID>)) t
    """
}
