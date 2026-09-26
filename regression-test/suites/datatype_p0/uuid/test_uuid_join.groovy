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

// Checklist: A05 D06 D09 D10 E05 E12 E14.
suite("test_uuid_join", "p0") {
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS uuid_query_paths_join"
    sql """
        CREATE TABLE uuid_query_paths_join (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_join VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    for (String hint : ["broadcast", "shuffle"]) {
        explain {
            sql "SELECT a.id FROM uuid_query_paths_join a JOIN [${hint}] uuid_query_paths_join b ON a.u = b.u"
            contains "HASH JOIN"
            contains hint == "broadcast" ? "INNER JOIN(BROADCAST)" : "INNER JOIN(PARTITIONED)"
        }
        order_qt_join """
            SELECT a.id, b.id, a.u FROM uuid_query_paths_join a
            JOIN [${hint}] uuid_query_paths_join b ON a.u = b.u ORDER BY a.id, b.id
        """
        order_qt_outer """
            SELECT a.id, b.id, a.u, b.u FROM uuid_query_paths_join a
            FULL OUTER JOIN [${hint}] (SELECT * FROM uuid_query_paths_join WHERE id <= 4) b
            ON a.u = b.u AND a.grp = b.grp ORDER BY a.id, b.id
        """
        order_qt_nullsafe_join """
            SELECT a.id, b.id FROM uuid_query_paths_join a JOIN [${hint}] uuid_query_paths_join b
            ON a.u <=> b.u AND a.grp = b.grp ORDER BY a.id, b.id
        """
    }
    for (String kind : ["LEFT SEMI", "LEFT ANTI"]) {
        order_qt_semi_anti """
            SELECT a.id, a.u FROM uuid_query_paths_join a ${kind} JOIN uuid_query_paths_join b
            ON a.u = b.u AND b.id < 5 ORDER BY a.id
        """
    }
}
