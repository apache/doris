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

suite("bucketed_hash_agg_num_buckets") {
    sql "set enable_sql_cache=false"
    sql "set enable_bucketed_hash_agg=false"
    sql "set be_number_for_test=1"
    sql "set agg_phase=1"
    sql "set bucketed_agg_min_input_rows=0"
    sql "set bucketed_agg_max_group_keys=0"
    sql "set bucketed_agg_high_card_threshold=1.0"

    sql "DROP TABLE IF EXISTS bucketed_agg_num_buckets_test"
    sql """
        CREATE TABLE bucketed_agg_num_buckets_test (
            id INT NOT NULL,
            distribution_key INT NOT NULL,
            numeric_grp INT NOT NULL,
            nullable_numeric_grp INT,
            non_null_grp VARCHAR(32) NOT NULL,
            grp VARCHAR(32),
            val BIGINT
        ) DUPLICATE KEY(id, distribution_key)
        DISTRIBUTED BY HASH(distribution_key) BUCKETS 4
        PROPERTIES('replication_num'='1')
    """
    // The distribution key is absent from every GROUP BY, preserving the hash-distribute
    // shape required by bucketed fusion even for the all-distinct id cases.
    // Multiple input batches exercise both local table growth and merging across sink instances.
    sql """
        INSERT INTO bucketed_agg_num_buckets_test
        SELECT number, number % 13, number % 11, IF(number % 8 = 0, NULL, number % 11),
               CAST(number % 7 AS VARCHAR(32)),
               IF(number % 8 = 0, NULL, CAST(number % 7 AS VARCHAR(32))),
               IF(number % 5 = 0, NULL, number)
        FROM numbers('number'='32768')
    """

    def queryCases = [
        string: """
            SELECT grp, COUNT(*), COUNT(val), SUM(val), MIN(val), MAX(val)
            FROM bucketed_agg_num_buckets_test GROUP BY grp
        """,
        numeric: """
            SELECT numeric_grp, COUNT(*), SUM(val)
            FROM bucketed_agg_num_buckets_test GROUP BY numeric_grp
        """,
        composite: """
            SELECT id % 3 AS k, grp, COUNT(*), SUM(val)
            FROM bucketed_agg_num_buckets_test GROUP BY k, grp
        """,
        // COUNT(*) alone selects the inline-count path for supported key types.
        count_numeric: """
            SELECT numeric_grp, COUNT(*)
            FROM bucketed_agg_num_buckets_test GROUP BY numeric_grp
        """,
        count_numeric_nullable: """
            SELECT nullable_numeric_grp, COUNT(*)
            FROM bucketed_agg_num_buckets_test GROUP BY nullable_numeric_grp
        """,
        count_string: """
            SELECT non_null_grp, COUNT(*)
            FROM bucketed_agg_num_buckets_test GROUP BY non_null_grp
        """,
        count_string_nullable: """
            SELECT grp, COUNT(*)
            FROM bucketed_agg_num_buckets_test GROUP BY grp
        """,
        // Keep 32768 distinct keys in the tested inner aggregate. The outer scalar
        // aggregate reduces expected output to one row and cannot itself be bucketed.
        distinct_numeric: """
            SELECT COUNT(*), SUM(k), SUM(cnt), MIN(cnt), MAX(cnt)
            FROM (
                SELECT id AS k, COUNT(*) AS cnt
                FROM bucketed_agg_num_buckets_test GROUP BY id
            ) t
        """,
        distinct_composite: """
            SELECT COUNT(*), SUM(k), SUM(cnt), SUM(total),
                   SUM(IF(grp IS NULL, cnt, 0))
            FROM (
                SELECT id AS k, grp, COUNT(*) AS cnt, SUM(val) AS total
                FROM bucketed_agg_num_buckets_test GROUP BY id, grp
            ) t
        """,
        // Avoid a constant/range predicate that could remove the aggregate from the plan.
        empty: """
            SELECT grp, COUNT(*), SUM(val) FROM bucketed_agg_num_buckets_test
            WHERE MD5(CAST(id AS STRING)) = 'bucketed-empty' GROUP BY grp
        """
    ]

    // Compute independent expected rows through regular aggregation once. Golden generation
    // must not accept a shared bucketed-aggregation bug as the expected result.
    sql "set parallel_pipeline_task_num=1"
    def referenceResults = [:]
    queryCases.each { kind, query ->
        explain {
            sql query
            notContains "BUCKETED AGGREGATE"
        }
        referenceResults[kind] = order_sql(query)
    }
    String unlimitedCountQuery = """
        SELECT id, COUNT(*) FROM bucketed_agg_num_buckets_test GROUP BY id
    """
    explain {
        sql unlimitedCountQuery
        notContains "BUCKETED AGGREGATE"
    }
    def limitReference = order_sql(unlimitedCountQuery).collectEntries { row ->
        [(row[0]): row[1]]
    }
    sql "set enable_bucketed_hash_agg=true"

    for (def parallelism : [1, 4]) {
        sql "set parallel_pipeline_task_num=${parallelism}"
        for (def numBuckets : [0, 1, 4, 16, 64, 256]) {
            sql "set bucketed_agg_num_buckets=${numBuckets}"
            String suffix = "${parallelism}_${numBuckets}"
            queryCases.each { kind, query ->
                explain {
                    sql query
                    contains "VBUCKETED AGGREGATE"
                    contains "buckets: ${numBuckets == 0 ? 'auto' : numBuckets}"
                }
                assertEquals(referenceResults[kind], order_sql(query))
                "order_qt_${kind}_${suffix}"(query)
            }

            // LimitAggToTopNAgg also adds TopN to an unordered LIMIT. Disable it only
            // for this query so the bucketed operator exercises early termination.
            // Any three groups are valid here; verify their invariants without a golden key order.
            String limitQuery = """
                SELECT /*+ SET_VAR(push_topn_to_agg=false) */ id, COUNT(*)
                FROM bucketed_agg_num_buckets_test GROUP BY id LIMIT 3
            """
            explain {
                sql limitQuery
                contains "VBUCKETED AGGREGATE"
                contains "buckets: ${numBuckets == 0 ? 'auto' : numBuckets}"
            }
            def limitRows = sql limitQuery
            assertEquals(3, limitRows.size())
            assertEquals(3, limitRows.collect { it[0] }.toSet().size())
            limitRows.each { row ->
                long key = (row[0] as Number).longValue()
                assertTrue(key >= 0 && key < 32768)
                assertEquals(1L, (row[1] as Number).longValue())
                assertEquals(limitReference[row[0]], row[1])
            }
        }
    }
    for (def invalid : [-1, 3, 255, 257, 512]) {
        test {
            sql "set bucketed_agg_num_buckets=${invalid}"
            exception "bucketed_agg_num_buckets must be 0 or a power of two between 1 and 256"
        }
    }
}
