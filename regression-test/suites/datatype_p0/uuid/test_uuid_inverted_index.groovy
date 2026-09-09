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

// Checklist: B10 F10 F16.
suite("test_uuid_inverted_index", "p0") {
    sql "SET enable_profile = true"
    // SQL result cache hits skip BE scans and do not contain index-filter counters.
    sql "SET enable_sql_cache = false"
    sql "SET profile_level = 2"
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    test {
        sql """CREATE TABLE uuid_index_v1_rejected (id INT,u UUID,INDEX idx(u) USING INVERTED)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES('replication_num'='1','inverted_index_storage_format'='V1')"""
        exception "Inverted index V1 is deprecated"
    }
    for (String format : ["V2", "V3", "SNII"]) {
        sql "DROP TABLE IF EXISTS uuid_index_paths"
        sql """CREATE TABLE uuid_index_paths (id INT,u UUID,a ARRAY<UUID>)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES('replication_num'='1', 'disable_auto_compaction'='true',
                          'inverted_index_storage_format'='${format}')"""
        sql """INSERT INTO uuid_index_paths VALUES
               (1,'7fffffff-ffff-ffff-ffff-ffffffffffff',['7fffffff-ffff-ffff-ffff-ffffffffffff']),
               (2,'80000000-0000-0000-0000-000000000000',['80000000-0000-0000-0000-000000000000',NULL]),
               (3,NULL,[]), (4,NULL,NULL)"""
        sql "ALTER TABLE uuid_index_paths ADD INDEX uuid_idx(u) USING INVERTED, ADD INDEX uuid_array_idx(a) USING INVERTED"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='uuid_index_paths' ORDER BY CreateTime DESC LIMIT 1"
            time 120
        }
        sql "BUILD INDEX uuid_idx ON uuid_index_paths"
        wait_for_last_build_index_finish("uuid_index_paths", 120000)
        sql "BUILD INDEX uuid_array_idx ON uuid_index_paths"
        wait_for_last_build_index_finish("uuid_index_paths", 120000)
        sql """INSERT INTO uuid_index_paths VALUES
               (5,'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',['ffffffff-ffff-ffff-ffff-ffffffffffff']),
               (6,'80000000000000000000000000000000',['80000000000000000000000000000000'])"""
        for (boolean compact : [false, true]) {
            if (compact && !isCloudMode()) {
                trigger_and_wait_compaction("uuid_index_paths", "full")
            }
            for (boolean enabled : [false, true]) {
                sql "SET enable_inverted_index_query = ${enabled}"
                String rangeToken = "uuid_index_range_${UUID.randomUUID()}"
                qt_range """/* ${rangeToken} */ SELECT id,u FROM uuid_index_paths
                            WHERE u >= '80000000-0000-0000-0000-000000000000' ORDER BY id"""
                checkProfileCounters(rangeToken, enabled ? ['RowsInvertedIndexFiltered'] : [],
                                 enabled ? [] : ['RowsInvertedIndexFiltered'])
                for (String predicate : ["u = CAST('80000000000000000000000000000000' AS UUID)",
                                         "u IN (CAST('80000000000000000000000000000000' AS UUID), CAST(REPEAT('f',32) AS UUID))"]) {
                    String scalarToken = "uuid_index_scalar_${UUID.randomUUID()}"
                    qt_scalar "/* ${scalarToken} */ SELECT id,u FROM uuid_index_paths WHERE ${predicate} ORDER BY id"
                    checkProfileCounters(scalarToken, enabled ? ['RowsInvertedIndexFiltered'] : [],
                                     enabled ? [] : ['RowsInvertedIndexFiltered'])
                }
                // Negated/range/OR predicates must preserve SQL NULL semantics in the bitmap.
                for (String predicate : ["u < CAST('80000000000000000000000000000000' AS UUID)",
                                         "u <= CAST('80000000000000000000000000000000' AS UUID)",
                                         "u > CAST('80000000000000000000000000000000' AS UUID)",
                                         "u != CAST('80000000000000000000000000000000' AS UUID)",
                                         "u NOT IN (CAST('80000000000000000000000000000000' AS UUID),CAST(REPEAT('f',32) AS UUID))",
                                         "u < CAST('80000000000000000000000000000000' AS UUID) OR u IS NULL"]) {
                    String token = "uuid_index_boundary_${UUID.randomUUID()}"
                    qt_boundaries "/* ${token} */ SELECT id,u FROM uuid_index_paths WHERE ${predicate} ORDER BY id"
                    checkProfileCounters(token, enabled ? ['RowsInvertedIndexFiltered'] : [],
                                     enabled ? [] : ['RowsInvertedIndexFiltered'])
                }
                // NULL poisons NOT IN; an empty result here does not prove index filtering.
                qt_not_in_null """SELECT id FROM uuid_index_paths
                                  WHERE u NOT IN (CAST('80000000000000000000000000000000' AS UUID),NULL) ORDER BY id"""
                String arrayToken = "uuid_index_array_${UUID.randomUUID()}"
                qt_array """/* ${arrayToken} */ SELECT id,a FROM uuid_index_paths
                            WHERE ARRAY_CONTAINS(a,CAST('80000000000000000000000000000000' AS UUID)) ORDER BY id"""
                checkProfileCounters(arrayToken, enabled ? ['RowsInvertedIndexFiltered'] : [],
                                 enabled ? [] : ['RowsInvertedIndexFiltered'])
                String nullToken = "uuid_index_null_${UUID.randomUUID()}"
                qt_nulls "/* ${nullToken} */ SELECT id FROM uuid_index_paths WHERE u IS NULL ORDER BY id"
                checkProfileCounters(nullToken, enabled ? ['RowsInvertedIndexFiltered'] : [],
                                 enabled ? [] : ['RowsInvertedIndexFiltered'])
            }
        }
    }
}
