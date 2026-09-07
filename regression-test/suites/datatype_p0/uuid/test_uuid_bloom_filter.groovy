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

// Checklist: F09 F07 J07.
suite("test_uuid_bloom_filter", "p0") {

    sql "DROP TABLE IF EXISTS uuid_bloom_filter"
    sql """CREATE TABLE uuid_bloom_filter (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1', 'bloom_filter_columns'='u',
                      'disable_auto_compaction'='true','storage_page_size'='4096')"""
    // Every rowset spans [0, ffff...]. ZoneMap cannot reject the interior probe value;
    // UUID is neither a key nor a distribution column, and there is no inverted index.
    for (int batch : [0, 1]) {
        sql """INSERT INTO uuid_bloom_filter SELECT number + ${batch * 8192},
               CAST(IF(number % 2 = 0, REPEAT('0',32), REPEAT('f',32)) AS UUID)
               FROM numbers('number'='8192')"""
    }
    sql """INSERT INTO uuid_bloom_filter VALUES
           (20000,'8abcdef0-1234-5678-9abc-def012345678'), (20001,NULL)"""
    // Cached predicate results must not bypass the mechanism measured below.
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_profile = true"
    sql "SET profile_level = 2"
    sql "SET enable_expr_zonemap_filter = false"
    String value = '8abcdef0-1234-5678-9abc-def012345678'
    String baseline = "uuid_bloom_baseline_${UUID.randomUUID()}"
    qt_baseline """/* ${baseline} */ SELECT id,u FROM uuid_bloom_filter
                   WHERE CONCAT(CAST(u AS STRING),'') = '${value}' ORDER BY id"""
    uuidCheckProfile(baseline, [], ['RowsBloomFilterFiltered'])
    for (boolean compact : [false, true]) {
        if (compact && !isCloudMode()) {
            trigger_and_wait_compaction('uuid_bloom_filter', 'full')
        }
        for (String predicate : ["u = CAST('${value}' AS UUID)",
                                 "u = CAST('8ABCDEF0123456789ABCDEF012345678' AS UUID)",
                                 "u IN (CAST('${value}' AS UUID), CAST('80000000000000000000000000000001' AS UUID))"]) {
            String token = "uuid_bloom_${UUID.randomUUID()}"
            qt_index "/* ${token} */ SELECT id,u FROM uuid_bloom_filter WHERE ${predicate} ORDER BY id"
            uuidCheckProfile(token, ['RowsBloomFilterFiltered'], ['RowsInvertedIndexFiltered', 'RowsKeyRangeFiltered'])
        }
    }
}
