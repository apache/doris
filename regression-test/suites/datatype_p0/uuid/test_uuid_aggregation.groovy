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

// Checklist: C04 D09 E06 E07.
suite("test_uuid_aggregation", "p0") {
    sql "DROP TABLE IF EXISTS uuid_query_paths_aggregation"
    sql """
        CREATE TABLE uuid_query_paths_aggregation (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_aggregation VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    order_qt_aggregate """
        SELECT grp, MIN(u), MAX(u), COUNT(u), COUNT(DISTINCT u),
               COUNT(DISTINCT u, id), NDV(u)
        FROM uuid_query_paths_aggregation GROUP BY grp ORDER BY grp
    """
    qt_empty_aggregate "SELECT MIN(u), MAX(u), COUNT(u), COUNT(DISTINCT u) FROM uuid_query_paths_aggregation WHERE id < 0"
    qt_all_null "SELECT MIN(u), MAX(u), COUNT(u), COUNT(DISTINCT u) FROM uuid_query_paths_aggregation WHERE u IS NULL"
    // The canonical unsigned 128-bit order crosses the signed 64-bit boundary.
    order_qt_uuid_group_order """
        SELECT CAST(u AS STRING), COUNT(*)
        FROM uuid_query_paths_aggregation GROUP BY u ORDER BY u
    """
    qt_uuid_count_distinct "SELECT COUNT(DISTINCT u) FROM uuid_query_paths_aggregation"

    for (int phase : [1, 2]) {
        sql "SET agg_phase = ${phase}"
        String query = "SELECT grp, MIN(u), MAX(u), COUNT(u) FROM uuid_query_paths_aggregation GROUP BY grp ORDER BY grp"
        explain {
            sql query
            if (phase == 2) {
                contains "VAGGREGATE (update serialize)"
            } else {
                notContains "VAGGREGATE (update serialize)"
            }
        }
        qt_aggregation_phase query
    }

    for (String aggregate : ["SUM", "AVG"]) {
        test {
            sql "SELECT ${aggregate}(u) FROM uuid_query_paths_aggregation"
            exception "${aggregate.toLowerCase()}"
        }
    }
}
