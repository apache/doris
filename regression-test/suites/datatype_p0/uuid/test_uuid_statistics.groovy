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

// Checklist: C09 C10.
suite("test_uuid_statistics", "p0") {
    sql "DROP TABLE IF EXISTS uuid_query_paths_statistics"
    sql """
        CREATE TABLE uuid_query_paths_statistics (id INT, u UUID, grp INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_query_paths_statistics VALUES
        (1, '00000000-0000-0000-0000-000000000000', 0),
        (2, '00112233445566778899AABBCCDDEEFF', 0),
        (3, '7fffffff-ffff-ffff-ffff-ffffffffffff', 1),
        (4, '80000000-0000-0000-0000-000000000000', 1),
        (5, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 2),
        (6, NULL, 2), (7, '00112233-4455-6677-8899-aabbccddeeff', 3), (8, NULL, 3)
    """
    sql "ANALYZE TABLE uuid_query_paths_statistics WITH SYNC"
    // Check persisted values without the changing analysis timestamp/trigger fields.
    def stats = sql_return_maparray("SHOW COLUMN STATS uuid_query_paths_statistics").find { it.column_name == 'u' }
    if (stats == null) {
        throw new IllegalStateException('ANALYZE did not produce UUID column statistics')
    }
    qt_statistics """SELECT ${stats.count}, ${stats.ndv}, ${stats.num_null},
                    ${stats.data_size}, ${stats.avg_size_byte}, '${stats.min}', '${stats.max}'"""
    order_qt_after_analyze "SELECT id FROM uuid_query_paths_statistics WHERE u >= '80000000-0000-0000-0000-000000000000' ORDER BY id"
}
