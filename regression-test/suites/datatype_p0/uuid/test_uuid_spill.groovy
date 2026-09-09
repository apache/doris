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

import org.apache.doris.regression.action.ProfileAction

suite("test_uuid_spill", "p0") {
    sql "DROP TABLE IF EXISTS uuid_spill_paths"
    sql """
        CREATE TABLE uuid_spill_paths (id BIGINT, u UUID, n UUID, a ARRAY<ARRAY<UUID>>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO uuid_spill_paths SELECT number,
            CAST(LPAD(HEX(number % 65536), 32, '0') AS UUID),
            IF(number % 7 = 0, NULL, CAST(LPAD(HEX(number % 65536), 32, '0') AS UUID)),
            ARRAY(ARRAY(CAST(LPAD(HEX(number % 65536), 32, '0') AS UUID), NULL), CAST(NULL AS ARRAY<UUID>))
        FROM numbers("number"="262144")
    """
    sql "SET enable_profile = true"
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    // Bucketed aggregation on a single BE does not use the aggregation spill operators.
    sql "SET enable_bucketed_hash_agg = false"
    sql "SET profile_level = 2"
    sql "SET topn_opt_limit_threshold = 0"
    sql "SET spill_min_revocable_mem = 524288"
    sql "SET parallel_pipeline_task_num = 1"
    def queries = [
        aggregate: """SELECT COUNT(*), SUM(c), MIN(u), MAX(u)
                      FROM (SELECT u, COUNT(*) c FROM uuid_spill_paths GROUP BY u) g""",
        join: """SELECT COUNT(*), MIN(a.u), MAX(b.n)
                 FROM uuid_spill_paths a JOIN [shuffle] uuid_spill_paths b ON a.u = b.u""",
        sort: """SELECT COUNT(*), MIN(u), MAX(u) FROM
                 (SELECT u FROM uuid_spill_paths ORDER BY u DESC LIMIT 131072) s""",
        nested_sort: """SELECT COUNT(*), MIN(CAST(a AS STRING)), MAX(CAST(a AS STRING)) FROM
                        (SELECT a FROM uuid_spill_paths ORDER BY a DESC LIMIT 131072) s"""
    ]
    try {
        for (boolean spill : [false, true]) {
            sql "SET enable_spill = ${spill}"
            sql "SET enable_force_spill = ${spill}"
            for (def entry : queries) {
                String token = "uuid_spill_${entry.key}_${UUID.randomUUID().toString()}"
                qt_spill_result "/* ${token} */ ${entry.value}"
                if (spill) {
                    String profileText = new ProfileAction(context).getProfileBySql(token, ["SpillWriteRows"])
                    // Prove that the result was read through the physical spill path.
                    if (!(profileText =~ /SpillWriteRows:\s*[1-9][0-9,.]*/).find()) {
                        throw new IllegalStateException("UUID ${entry.key} did not spill rows: ${profileText}")
                    }
                }
            }
        }
    } finally {
        sql "SET enable_force_spill = false"
        sql "SET enable_spill = false"
    }
}
