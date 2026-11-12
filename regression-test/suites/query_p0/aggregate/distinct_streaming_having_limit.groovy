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

suite("distinct_streaming_having_limit") {
    sql "DROP TABLE IF EXISTS distinct_streaming_having_limit"
    sql """
        CREATE TABLE distinct_streaming_having_limit (k INT NOT NULL)
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Two groups are sufficient: HAVING rejects the first and keeps the second.
    sql "INSERT INTO distinct_streaming_having_limit VALUES (1), (2)"

    // Keep one ordered input stream. Disable the other final aggregation paths.
    // In particular, the final distinct streaming operator does not support spill.
    sql "SET parallel_pipeline_task_num = 1"
    sql "SET enable_bucketed_hash_agg = false"
    sql "SET enable_spill = false"
    // Execute each operator path instead of reusing the reference query's cached result.
    sql "SET enable_sql_cache = false"

    // ORDER BY and the inner LIMIT make group 1 arrive first.
    // RAND() keeps HAVING on the aggregate node. Its value is in [0, 1), so
    // the predicate is always false for k = 1 and always true for k = 2.
    // The final LIMIT must therefore return 2, regardless of the random value.
    def query = """
        SELECT k
        FROM (SELECT k FROM distinct_streaming_having_limit ORDER BY k LIMIT 2) s
        GROUP BY k
        HAVING RAND() < CAST(k - 1 AS DOUBLE)
        LIMIT 1
    """

    for (def batchSize : [1, 1024]) {
        sql "SET batch_size = ${batchSize}"
        // Use the ordinary aggregate as the reference for each batch size.
        sql "SET enable_distinct_streaming_aggregation = false"
        order_qt_reference query

        sql "SET enable_distinct_streaming_aggregation = true"
        explain {
            sql query
            check { plan ->
                // Require HAVING and LIMIT on the same final aggregate node.
                // Each intervening node detail starts with a vertical bar.
                (plan =~ /VAGGREGATE \([^\n]*finalize\)[^\n]*\n(?:\s*\|[^\n]*\n)*?\s*\|\s*having:[^\n]*\n(?:\s*\|[^\n]*\n)*?\s*\|\s*limit: 1\b/).find()
            }
        }
        // Cover both one row per batch and the default batch size.
        order_qt_streaming query
    }
}
