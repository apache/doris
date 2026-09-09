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

// Checklist: F08 F07 J07.
suite("test_uuid_zone_map", "p0") {

    sql "DROP TABLE IF EXISTS uuid_zone_map"
    sql """CREATE TABLE uuid_zone_map (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1','disable_auto_compaction'='true')"""
    // Separate flushes give disjoint segment bounds, including one all-NULL segment.
    for (String value : ['00000000-0000-0000-0000-000000000000',
                         '7fffffff-ffff-ffff-ffff-ffffffffffff',
                         '80000000-0000-0000-0000-000000000000',
                         'ffffffff-ffff-ffff-ffff-ffffffffffff']) {
        sql "INSERT INTO uuid_zone_map SELECT number, CAST('${value}' AS UUID) FROM numbers('number'='4096')"
    }
    sql "INSERT INTO uuid_zone_map SELECT number,NULL FROM numbers('number'='4096')"
    // Cached predicate results must not bypass the mechanism measured below.
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"
    sql "SET profile_level = 2"
    sql "SET enable_expr_zonemap_filter = false"
    for (String comparison : ['>', '>=', '<', '<=', '=']) {
        String token = "uuid_zone_${UUID.randomUUID()}"
        qt_native """/* ${token} */ SELECT COUNT(*),MIN(u),MAX(u) FROM uuid_zone_map
                     WHERE u ${comparison} CAST('80000000-0000-0000-0000-000000000000' AS UUID)"""
        uuidCheckProfile(token, ['RowsStatsFiltered'], ['RowsBloomFilterFiltered', 'RowsInvertedIndexFiltered', 'RowsKeyRangeFiltered'])
        String baseline = "uuid_zone_baseline_${UUID.randomUUID()}"
        qt_reference """/* ${baseline} */ SELECT COUNT(*),MIN(u),MAX(u) FROM uuid_zone_map
                        WHERE CONCAT(CAST(u AS STRING),'') ${comparison} '80000000-0000-0000-0000-000000000000'"""
        uuidCheckProfile(baseline, [], ['RowsStatsFiltered'])
    }
    String nullToken = "uuid_zone_null_${UUID.randomUUID()}"
    qt_null "/* ${nullToken} */ SELECT COUNT(*) FROM uuid_zone_map WHERE u IS NULL"
    uuidCheckProfile(nullToken, ['RowsStatsFiltered'], ['RowsBloomFilterFiltered','RowsInvertedIndexFiltered'])

}
