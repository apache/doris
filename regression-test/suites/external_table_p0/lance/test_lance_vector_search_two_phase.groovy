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

suite("test_lance_vector_search_two_phase", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector search two-phase test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_two_phase"
    String tableName = "${catalogName}.doris.vs_ivf_pq_f32"
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"

    // This frozen fixture has one physical index segment covering two fragments. With top_k=5 and
    // offset=1, the index split contributes at most six phase-one candidates and the global TopN
    // keeps rows 2..6.
    String indexedWithOffset = """vector_search(
            "table"="${tableName}",
            "column"="embedding",
            "query_vector"="${headQuery}",
            "top_k"="5",
            "offset"="1",
            "metric"="l2",
            "nprobes"="4",
            "refine_factor"="10",
            "use_index"="true")"""
    String resultQuery = """
            SELECT row_id, category, label, _distance
            FROM ${indexedWithOffset}
            ORDER BY _distance
        """

    sql "DROP CATALOG IF EXISTS `${catalogName}`"
    try {
        sql """
            CREATE CATALOG `${catalogName}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """
        sql "SET enable_file_scanner_v2 = true"
        // The same SQL is executed with two different materialization settings below. Disable
        // both caches so the second execution cannot reuse the first execution's result.
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"

        // Plan contract with TopN lazy materialization enabled:
        //   phase 1: Lance reads embedding for ANN, but outputs only _distance and the hidden row ID
        //   global TopN: OFFSET 1 / LIMIT 5
        //   phase 2: fetch row_id, category and label by the hidden row ID
        sql "SET topn_lazy_materialization_threshold = 1024"
        explain {
            sql "verbose ${resultQuery}"
            check { explainString ->
                assertTrue(explainString.contains("VMaterializeNode"))
                assertTrue(explainString.contains("VTOP-N"))
                assertTrue(explainString.contains(
                        "row_ids: [__DORIS_GLOBAL_ROWID_COL__vector_search]"))
                assertTrue(explainString.contains("isTopMaterializeNode: true"))
                assertTrue(explainString.contains("limit: 5"))
                assertTrue(explainString.contains("offset: 1"))
                assertTrue(explainString.contains("externalSearchType=VECTOR"))
                assertTrue(explainString.contains("lanceSearchFragments=2"))
                assertTrue(explainString.contains("lanceSearchUnindexedFragments=0"))
                assertTrue(explainString.contains("lanceSearchIndexSegments=1"))
                assertTrue(explainString.contains("lanceSearchIndexFragments=2"))
                String lazyColumns = explainString.readLines()
                        .find { line -> line.contains("column_descs_lists") }
                assertTrue(lazyColumns != null, "missing phase-two column_descs_lists")
                assertTrue(lazyColumns.contains("row_id"))
                assertTrue(lazyColumns.contains("category"))
                assertTrue(lazyColumns.contains("label"))
                assertFalse(lazyColumns.contains("embedding"))
                return true
            }
        }

        qt_two_phase_execution "${resultQuery}"

        // Turning the threshold off removes both the Materialization node and the hidden row ID.
        // The user-visible result must remain identical to the two-phase result.
        sql "SET topn_lazy_materialization_threshold = -1"
        explain {
            sql "verbose ${resultQuery}"
            notContains "VMaterializeNode"
            notContains "__DORIS_GLOBAL_ROWID_COL__vector_search"
            contains "externalSearchType=VECTOR"
            contains "lanceSearchFragments=2"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
            contains "lanceSearchIndexFragments=2"
        }
        qt_one_phase_execution "${resultQuery}"

        sql "SET topn_lazy_materialization_threshold = 1024"

        // TVF filter is a Lance prefilter. Lance reads category while selecting ANN candidates
        // and removes ineligible rows before nearest(), so the three nearest category='odd' rows
        // are row IDs 2, 4 and 6. It does not become a Doris residual predicate.
        String prefilterQuery = """
            SELECT row_id, category, label, _distance
            FROM vector_search(
                "table"="${tableName}",
                "column"="embedding",
                "query_vector"="${headQuery}",
                "top_k"="3",
                "filter"="category = 'odd'",
                "metric"="l2",
                "nprobes"="4",
                "refine_factor"="10",
                "use_index"="true")
            ORDER BY _distance
        """
        explain {
            sql "verbose ${prefilterQuery}"
            contains "VMaterializeNode"
            notContains "predicates:"
        }
        qt_prefilter_execution "${prefilterQuery}"

        // With use_index=true, the one physical index segment returns rows 1..3 across both
        // fragments. Doris evaluates category='odd' in phase one and keeps only row 2.
        String indexedPostfilterQuery = """
            SELECT row_id, category, label, _distance
            FROM vector_search(
                "table"="${tableName}",
                "column"="embedding",
                "query_vector"="${headQuery}",
                "top_k"="3",
                "metric"="l2",
                "nprobes"="4",
                "refine_factor"="10",
                "use_index"="true")
            WHERE category = 'odd'
            ORDER BY _distance
        """
        explain {
            sql "verbose ${indexedPostfilterQuery}"
            check { explainString ->
                assertTrue(explainString.contains("VMaterializeNode"))
                assertFalse(explainString.contains("VSELECT"))
                assertTrue(explainString.contains("predicates:"))
                assertTrue(explainString.contains("category"))
                assertTrue(explainString.contains("lanceSearchUnindexedFragments=0"))
                assertTrue(explainString.contains("lanceSearchIndexSegments=1"))
                assertTrue(explainString.contains("lanceSearchIndexFragments=2"))
                String lazyColumns = explainString.readLines()
                        .find { line -> line.contains("column_descs_lists") }
                assertTrue(lazyColumns != null, "missing phase-two column_descs_lists")
                assertTrue(lazyColumns.contains("row_id"))
                assertTrue(lazyColumns.contains("label"))
                assertFalse(lazyColumns.contains("category"),
                        "postfilter column category must be read in phase one")
                return true
            }
        }
        qt_postfilter_use_index_true "${indexedPostfilterQuery}"

        // With use_index=false, each fragment is a flat-search split and returns three candidates.
        // Postfilter keeps rows 2 and 514 before global TopN; no replacement rows are requested.
        String flatPostfilterQuery = """
            SELECT row_id, category, label, _distance
            FROM vector_search(
                "table"="${tableName}",
                "column"="embedding",
                "query_vector"="${headQuery}",
                "top_k"="3",
                "metric"="l2",
                "use_index"="false")
            WHERE category = 'odd'
            ORDER BY _distance
        """
        explain {
            sql "verbose ${flatPostfilterQuery}"
            contains "VMaterializeNode"
            contains "predicates:"
            contains "lanceSearchUnindexedFragments=2"
            contains "lanceSearchIndexSegments=0"
            contains "lanceSearchIndexFragments=0"
        }
        qt_postfilter_use_index_false "${flatPostfilterQuery}"
    } finally {
        // sql "DROP CATALOG IF EXISTS `${catalogName}`"
    }
}
