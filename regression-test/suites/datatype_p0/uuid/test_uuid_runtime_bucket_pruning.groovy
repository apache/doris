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

// Checklist: B08 B09 C08 E11.
suite("test_uuid_runtime_bucket_pruning", "p0") {

    sql "SET enable_runtime_filter_prune = false"
    sql "SET enable_runtime_filter_partition_prune = false"
    sql "SET enable_runtime_filter_bucket_prune = false"
    sql "SET runtime_filter_wait_infinitely = true"
    sql "SET runtime_filter_max_in_num = 1024"
    sql "SET disable_join_reorder = true"
    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"
    sql "SET profile_level = 2"
    sql "SET enable_condition_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET parallel_pipeline_task_num = 1"

    sql "DROP TABLE IF EXISTS uuid_rf_bucket_fact"
    sql """CREATE TABLE uuid_rf_bucket_fact (u UUID NOT NULL,id INT)
           DUPLICATE KEY(u)
           DISTRIBUTED BY HASH(u) BUCKETS 8 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_rf_bucket_fact VALUES
           ('7fffffff-ffff-ffff-ffff-ffffffffffff',1),
           ('80000000-0000-0000-0000-000000000000',2),
           ('ffffffff-ffff-ffff-ffff-ffffffffffff',3)"""
    sql "DROP TABLE IF EXISTS uuid_rf_bucket_dim"
    sql """CREATE TABLE uuid_rf_bucket_dim (u UUID NOT NULL)
           DUPLICATE KEY(u) DISTRIBUTED BY HASH(u) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_rf_bucket_dim VALUES ('80000000000000000000000000000000')"
    sql "SET runtime_filter_type = 'IN'"
    String query = """SELECT f.id,f.u FROM uuid_rf_bucket_fact f
                      JOIN [broadcast] uuid_rf_bucket_dim d ON f.u=d.u ORDER BY f.id"""
    // The build-side value comes from a table, not a literal predicate propagated by FE.
    explain {
        sql "verbose ${query}"
        contains "tablets=8/8"
    }
    for (boolean enabled : [false,true]) {
        sql "SET enable_runtime_filter_bucket_prune = ${enabled}"
        String token = "uuid_rf_bucket_${UUID.randomUUID()}"
        qt_result "/* ${token} */ ${query}"
        uuidCheckProfile(token, enabled ? ['BucketsPrunedByRuntimeFilter'] : [], enabled ? [] : ['BucketsPrunedByRuntimeFilter'])
    }

}
