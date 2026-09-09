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

// Checklist: C07 E01 E03 F07 J07.
suite("test_uuid_predicate_pushdown", "p0") {

    sql "DROP TABLE IF EXISTS uuid_predicate_pushdown"
    sql """CREATE TABLE uuid_predicate_pushdown (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1')"""
    // All pages contain all boundaries, so a matching segment cannot be eliminated by ZoneMap.
    // UUID is a value column without an index: row filtering must execute in the BE scanner.
    sql """INSERT INTO uuid_predicate_pushdown SELECT number,
           CASE number % 5 WHEN 0 THEN CAST(NULL AS UUID)
             WHEN 1 THEN CAST('00000000-0000-0000-0000-000000000000' AS UUID)
             WHEN 2 THEN CAST('7fffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)
             WHEN 3 THEN CAST('80000000-0000-0000-0000-000000000000' AS UUID)
             ELSE CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID) END
           FROM numbers('number'='16385')"""
    // Cached predicate results must not bypass the mechanism measured below.
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"
    sql "SET profile_level = 2"
    sql "SET enable_expr_zonemap_filter = false"
    String value = '80000000-0000-0000-0000-000000000000'
    for (String comparison : ['=', '!=', '<', '<=', '>', '>=']) {
        String token = "uuid_predicate_${UUID.randomUUID()}"
        qt_pushdown """/* ${token} */ SELECT COUNT(*),SUM(id),MIN(u),MAX(u)
                       FROM uuid_predicate_pushdown WHERE u ${comparison} CAST('${value}' AS UUID)"""
        uuidCheckProfile(token, ['RowsVectorPredFiltered'],
                         ['RowsStatsFiltered','RowsKeyRangeFiltered','RowsBloomFilterFiltered','RowsInvertedIndexFiltered'])
        qt_reference """SELECT COUNT(*),SUM(id),MIN(u),MAX(u) FROM uuid_predicate_pushdown
                        WHERE CONCAT(CAST(u AS STRING),'') ${comparison} '${value}'"""
    }
    for (String predicate : ["u IN (CAST('${value}' AS UUID),CAST(REPEAT('f',32) AS UUID))",
                             "u NOT IN (CAST('${value}' AS UUID))",
                             "u NOT IN (CAST('${value}' AS UUID),CAST(REPEAT('f',32) AS UUID))", 'u IS NULL', 'u IS NOT NULL']) {
        String token = "uuid_predicate_set_${UUID.randomUUID()}"
        qt_sets "/* ${token} */ SELECT COUNT(*),SUM(id),MIN(u),MAX(u) FROM uuid_predicate_pushdown WHERE ${predicate}"
        uuidCheckProfile(token, [predicate == "u NOT IN (CAST('${value}' AS UUID))" ? 'RowsVectorPredFiltered' : 'RowsShortCircuitPredFiltered'],
                         ['RowsStatsFiltered','RowsInvertedIndexFiltered'])
    }

}
