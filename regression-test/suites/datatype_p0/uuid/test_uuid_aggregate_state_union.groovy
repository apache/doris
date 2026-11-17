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

suite("test_uuid_aggregate_state_union", "p0,nonConcurrent") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "SET enable_agg_state=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    // Keep several groups in the same output block, including the separately appended NULL key.
    sql "SET parallel_pipeline_task_num=1"
    sql "SET disable_streaming_preaggregations=true"
    sql "SET batch_size=4064"
    sql "SET bucketed_agg_min_input_rows=0"
    sql "DROP TABLE IF EXISTS uuid_aggregate_state_union"
    sql """CREATE TABLE uuid_aggregate_state_union (id INT, u UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_aggregate_state_union VALUES
           (0,NULL),(0,NULL),
           (1,NULL),(1,'00000000-0000-0000-0000-000000000000'),
           (1,'550e8400-e29b-41d4-a716-446655440000'),
           (2,'80000000-0000-0000-0000-000000000000'),
           (2,'ffffffff-ffff-ffff-ffff-ffffffffffff'),
           (NULL,'00112233-4455-6677-8899-aabbccddeeff')"""

    for (boolean spill : [false, true]) {
        sql "SET enable_spill=${spill}"
        for (int phase : [1, 2]) {
            sql "SET agg_phase=${phase}"
            for (String mode : ['fe', 'be', 'runtime']) {
                sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
                sql "SET enable_fold_constant_by_be=${mode == 'be'}"
                List<String> inputs = matrix.rows().collect { "NULLABLE(${it.u})" } + ['u']
                inputs.eachWithIndex { String input, int sample ->
                    String states = """SELECT id, MAX_UNION(MAX_STATE(${input})) hi,
                        MIN_UNION(MIN_STATE(${input})) lo, COUNT_UNION(COUNT_STATE(${input})) n
                        FROM uuid_aggregate_state_union GROUP BY id"""
                    explain {
                        sql "SELECT MAX_MERGE(hi),MIN_MERGE(lo),COUNT_MERGE(n) FROM (${states}) states"
                        contains "count_union"
                    }
                    String prefix = "spill_${spill}_p${phase}_${mode}_${sample}"
                    // Check individual group counts as well as their total: retaining only the
                    // final serialized state can otherwise silently lose non-NULL counts.
                    "qt_${prefix}_groups" """SELECT id,MAX_MERGE(hi),MIN_MERGE(lo),COUNT_MERGE(n)
                        FROM (${states}) states GROUP BY id ORDER BY id"""
                    "qt_${prefix}_total" """SELECT MAX_MERGE(hi),MIN_MERGE(lo),COUNT_MERGE(n)
                        FROM (${states}) states"""
                }
            }
        }
    }
}
