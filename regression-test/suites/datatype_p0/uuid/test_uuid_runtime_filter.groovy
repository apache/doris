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

// Checklist: E11 E12 F07.
suite("test_uuid_runtime_filter", "p0") {

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
    sql "SET parallel_pipeline_task_num = 1"

    sql "DROP TABLE IF EXISTS uuid_runtime_filter_fact"
    sql """CREATE TABLE uuid_runtime_filter_fact (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_runtime_filter_fact SELECT number,
           CASE number % 4 WHEN 0 THEN CAST(NULL AS UUID)
             WHEN 1 THEN CAST('7fffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)
             WHEN 2 THEN CAST('80000000-0000-0000-0000-000000000000' AS UUID)
             ELSE CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID) END
           FROM numbers('number'='16384')"""
    sql "DROP TABLE IF EXISTS uuid_runtime_filter_dim"
    sql """CREATE TABLE uuid_runtime_filter_dim (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_runtime_filter_dim VALUES (1,'80000000000000000000000000000000')"
    String query = """SELECT COUNT(*),SUM(f.id),MIN(f.u),MAX(f.u) FROM uuid_runtime_filter_fact f
                      JOIN [broadcast] uuid_runtime_filter_dim d ON f.u = d.u"""
    sql "SET runtime_filter_type = 0"
    qt_disabled query
    for (String type : ['IN', 'BLOOM_FILTER', 'MIN_MAX', 'IN_OR_BLOOM_FILTER']) {
        sql "SET runtime_filter_type = '${type}'"
        explain {
            sql "verbose ${query}"
            contains "runtime filters:"
        }
        String token = "uuid_runtime_filter_${UUID.randomUUID()}"
        qt_enabled "/* ${token} */ ${query}"
        uuidCheckProfile(token, ['RF0 InputRows', 'RF0 FilterRows'], ['RowsInvertedIndexFiltered'])
    }

}
