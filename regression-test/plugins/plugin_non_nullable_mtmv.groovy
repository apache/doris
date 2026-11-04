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

import org.apache.doris.regression.suite.Suite

Suite.metaClass.checkNonNullableMvRewrite = { String source, columns, String mv ->
    Suite suite = delegate as Suite
    List<String> names = columns instanceof List ? columns : [columns]
    String projection = names.collect { "`${it}`" }.join(", ")
    String asserted = names.collect { "non_nullable(`${it}`) AS `${it}`" }.join(", ")
    String previousRewrite = suite.sql("SELECT @@enable_materialized_view_rewrite")[0][0].toString()
    try {
        suite.sql("SET enable_materialized_view_rewrite = false")
        def schema = suite.sql("DESC ${source}")
        names.each { name ->
            suite.assertTrue(schema.any { it[0] == name && it[2] == "Yes" },
                    "The fixture must expose a nullable source column: ${name}")
        }
        def expected = suite.sql("SELECT ${projection} FROM ${source} ORDER BY 1")
        suite.assertFalse(expected.isEmpty(), "The source fixture must exercise materialized rows")
        suite.sql("DROP MATERIALIZED VIEW IF EXISTS ${mv}")
        suite.sql("""
            CREATE MATERIALIZED VIEW ${mv}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ("replication_num" = "1", "grace_period" = "3600000")
            AS SELECT ${asserted} FROM ${source}
        """)
        if (names.contains("vec")) {
            suite.sql("""CREATE INDEX ${mv}_ann ON ${mv}(vec) USING ANN
                PROPERTIES ("index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "2")""")
            String indexJobs = "SHOW ALTER TABLE COLUMN WHERE TableName = '${mv}' ORDER BY CreateTime DESC LIMIT 1"
            if (!suite.sql(indexJobs).isEmpty()) {
                suite.waitForSchemaChangeDone {
                    sql indexJobs
                    time 600
                }
            }
            suite.sql("SYNC")
        }
        suite.sql("REFRESH MATERIALIZED VIEW ${mv} COMPLETE")
        suite.waitingMTMVTaskFinishedByMvName(mv)
        suite.sql("SET enable_materialized_view_rewrite = true")
        // Keep non-versioned sources eligible while the fixture remains unchanged.
        String query = "SELECT /*+ use_mv(${mv}) */ ${projection} FROM ${source} ORDER BY 1"
        suite.mv_rewrite_success(query, mv)
        suite.assertEquals(expected.toString(), suite.sql(query).toString())
        String wrapped = "SELECT /*+ use_mv(${mv}) */ ${asserted} " +
                "FROM ${source} ORDER BY 1"
        suite.mv_rewrite_success(wrapped, mv)
        suite.assertEquals(expected.toString(), suite.sql(wrapped).toString())
        String filtered = "SELECT /*+ use_mv(${mv}) */ ${projection} FROM ${source} " +
                "WHERE `${names[0]}` IS NOT NULL ORDER BY 1"
        suite.mv_rewrite_success(filtered, mv)
        suite.assertEquals(expected.toString(), suite.sql(filtered).toString())
        if (names.contains("vec")) {
            String ann = "SELECT /*+ use_mv(${mv}) */ id, " +
                    "l2_distance_approximate(vec, [0.0, 0.0]) AS distance FROM ${source} ORDER BY distance LIMIT 2"
            suite.mv_rewrite_success(ann, mv)
            suite.explain {
                sql ann
                contains "ANN SORT INFO"
            }
            def actual = suite.sql(ann)
            suite.sql("SET enable_materialized_view_rewrite = false")
            suite.assertEquals(suite.sql(ann).toString(), actual.toString())
        }
    } finally {
        suite.sql("SET enable_materialized_view_rewrite = ${previousRewrite}")
    }
}
