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

// Checklist: D09 D11 E11 E12.
suite("test_uuid_runtime_filter_merge", "p0") {
    sql "SET enable_runtime_filter_prune = false"
    sql "SET enable_runtime_filter_partition_prune = false"
    sql "SET enable_runtime_filter_bucket_prune = false"
    sql "SET runtime_filter_wait_infinitely = true"
    sql "SET runtime_filter_max_in_num = 1024"
    sql "SET disable_join_reorder = true"
    sql "SET enable_profile = true"
    sql "SET profile_level = 2"
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET parallel_pipeline_task_num = 2"
    sql "DROP TABLE IF EXISTS uuid_rf_merge_fact"
    sql """CREATE TABLE uuid_rf_merge_fact (id INT,u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 6 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_rf_merge_fact SELECT number,
           CASE number % 8 WHEN 0 THEN CAST(NULL AS UUID)
             WHEN 1 THEN CAST(REPEAT('0',32) AS UUID)
             WHEN 2 THEN CAST(REPEAT('f',32) AS UUID)
             ELSE CAST(CONCAT(IF(number % 2 = 0,'7fffffffffffffff','8000000000000000'),
                              LPAD(HEX(number % 512),16,'0')) AS UUID) END
           FROM numbers('number'='16384')"""
    sql "DROP TABLE IF EXISTS uuid_rf_merge_dim"
    sql """CREATE TABLE uuid_rf_merge_dim (id INT,u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 6 PROPERTIES('replication_num'='1')"""
    // Enough distinct keys populate every build fragment; duplicates and NULL are retained.
    sql """INSERT INTO uuid_rf_merge_dim SELECT number,
           IF(number % 17 = 0,NULL,
              CAST(CONCAT(IF(number % 2 = 0,'7FFFFFFFFFFFFFFF','8000000000000000'),
                          LPAD(HEX(number),16,'0')) AS UUID))
           FROM numbers('number'='256')"""
    sql "INSERT INTO uuid_rf_merge_dim SELECT * FROM uuid_rf_merge_dim"
    String query = """SELECT COUNT(*),COUNT(DISTINCT f.id),SUM(f.id),MIN(f.u),MAX(f.u)
                      FROM uuid_rf_merge_fact f JOIN [shuffle] uuid_rf_merge_dim d ON f.u=d.u"""
    sql "SET runtime_filter_type = 0"
    qt_disabled query
    for (def spec : [['IN',1024], ['BLOOM_FILTER',1024], ['MIN_MAX',1024],
                     ['IN_OR_BLOOM_FILTER',1024], ['IN_OR_BLOOM_FILTER',4]]) {
        String type = spec[0]
        sql "SET runtime_filter_max_in_num = ${spec[1]}"
        sql "SET runtime_filter_type = '${type}'"
        explain {
            sql "verbose ${query}"
            contains "INNER JOIN(PARTITIONED)"
            contains "runtime filters:"
        }
        String token = "uuid_rf_merge_${UUID.randomUUID()}"
        qt_enabled "/* ${token} */ ${query}"
        String profile = uuidCheckProfile(token, ['RF0 InputRows', 'RF0 FilterRows'], ['RowsInvertedIndexFiltered'])
        String effectiveType = type == 'IN_OR_BLOOM_FILTER'
                ? "IN_OR_BLOOM_FILTER(${spec[1] == 4 ? 'BLOOM_FILTER' : 'IN_FILTER'})"
                : (type == 'IN' ? 'IN_FILTER' : (type == 'MIN_MAX' ? 'MINMAX_FILTER' : type))
        if (!profile.contains('mode: GLOBAL') || !profile.contains("type: ${effectiveType}, column_type: UUID")) {
            throw new IllegalStateException("UUID runtime filter did not take the global merge path: ${profile}")
        }
    }
}
