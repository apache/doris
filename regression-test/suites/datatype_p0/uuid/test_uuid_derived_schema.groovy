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

// Checklist: A01 B02 B03 B11 I05.
suite("test_uuid_derived_schema", "p0") {
    sql "DROP TABLE IF EXISTS uuid_query_paths_derived_schema"
    sql """
        CREATE TABLE uuid_query_paths_derived_schema (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_derived_schema VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    sql "DROP TABLE IF EXISTS uuid_nested_paths_derived_schema"
    sql """
        CREATE TABLE uuid_nested_paths_derived_schema (
            id INT, a ARRAY<UUID>, m MAP<UUID, ARRAY<UUID>>,
            s STRUCT<k:UUID, a:ARRAY<UUID>>, aa ARRAY<ARRAY<UUID>>
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_nested_paths_derived_schema
        SELECT id, ARRAY(u, NULL), MAP(u, ARRAY(NULL, u)),
               NAMED_STRUCT('k', u, 'a', ARRAY(u, NULL)), ARRAY(ARRAY(u, NULL), ARRAY(u))
        FROM uuid_query_paths_derived_schema WHERE u IS NOT NULL
    """
    sql "INSERT INTO uuid_nested_paths_derived_schema VALUES (9, NULL, NULL, NULL, NULL), (10, [], {}, NULL, [])"
    sql "DROP TABLE IF EXISTS uuid_derived_paths_derived_schema"
    sql "CREATE TABLE uuid_derived_paths_derived_schema PROPERTIES('replication_num'='1') AS SELECT * FROM uuid_nested_paths_derived_schema"
    sql "DROP TABLE IF EXISTS uuid_like_paths_derived_schema"
    sql "CREATE TABLE uuid_like_paths_derived_schema LIKE uuid_derived_paths_derived_schema"
    sql "INSERT INTO uuid_like_paths_derived_schema SELECT * FROM uuid_derived_paths_derived_schema"
    order_qt_derived "SELECT * FROM uuid_like_paths_derived_schema ORDER BY id"
    sql "DROP VIEW IF EXISTS uuid_query_view_derived_schema"
    sql "CREATE VIEW uuid_query_view_derived_schema AS SELECT id, u FROM uuid_query_paths_derived_schema"
    order_qt_view "WITH q AS (SELECT * FROM uuid_query_view_derived_schema) SELECT * FROM q ORDER BY id"
    order_qt_metadata """
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME IN ('uuid_query_paths_derived_schema', 'uuid_derived_paths_derived_schema', 'uuid_like_paths_derived_schema')
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
}
