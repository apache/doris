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

suite("parallel_rowid_fetch_v2") {
    sql "DROP TABLE IF EXISTS parallel_rowid_fetch_v2"
    sql """
        CREATE TABLE parallel_rowid_fetch_v2 (
            id INT NOT NULL,
            sort_key INT NOT NULL,
            payload STRING NULL,
            values_array ARRAY<INT> NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    // Separate rowsets and buckets give each request multiple segment groups.
    for (int batch = 0; batch < 3; ++batch) {
        sql """
            INSERT INTO parallel_rowid_fetch_v2
            SELECT number + ${batch * 200}, (number * 37) % 101,
                   IF(number % 7 = 0, NULL, CONCAT('payload-', CAST(number AS STRING))),
                   ARRAY(CAST(number AS INT), NULL, CAST(number + 1 AS INT))
            FROM numbers("number" = "200")
        """
    }
    sql "DROP TABLE IF EXISTS parallel_rowid_fetch_v2_row_store"
    sql """
        CREATE TABLE parallel_rowid_fetch_v2_row_store (
            id INT NOT NULL, sort_key INT NOT NULL, payload STRING NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES ("replication_num" = "1", "store_row_column" = "true")
    """
    sql """INSERT INTO parallel_rowid_fetch_v2_row_store
           SELECT id, sort_key, payload FROM parallel_rowid_fetch_v2"""
    sql "SYNC"
    sql "SET topn_opt_limit_threshold = 1024"

    def queries = [
        """SELECT id, payload, payload, values_array FROM parallel_rowid_fetch_v2
           ORDER BY sort_key, id LIMIT 137 OFFSET 3""",
        // The join repeats source RowIDs and interleaves rows from different segments.
        """SELECT t.id, t.payload, t.values_array, d.x
           FROM parallel_rowid_fetch_v2 t CROSS JOIN (SELECT 1 x UNION ALL SELECT 2 x) d
           ORDER BY t.sort_key, t.id, d.x LIMIT 137""",
        """SELECT id, payload FROM parallel_rowid_fetch_v2_row_store
           ORDER BY sort_key, id LIMIT 137""",
        """SELECT id, payload FROM parallel_rowid_fetch_v2
           WHERE id = -1 ORDER BY sort_key, id LIMIT 10"""
    ]
    sql "SET enable_two_phase_read_opt = false"
    sql "SET topn_lazy_materialization_threshold = 0"
    def expected = queries.collect { sql(it) }
    sql "SET enable_two_phase_read_opt = true"
    sql "SET topn_lazy_materialization_threshold = 1024"
    explain {
        sql queries[0]
        contains "VMaterializeNode"
    }
    for (int batch : [0, -1, 1, 2, 100000]) {
        sql "SET rowid_fetch_parallel_batch_rows = ${batch}"
        queries.eachWithIndex { query, i ->
            assertEquals(expected[i], sql(query))
        }
    }
}
