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

suite("test_lance_runtime_filter_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance runtime-filter test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_runtime_filter_pushdown"
    String buildTable = "test_lance_runtime_filter_build"
    String internalDb = context.dbName
    String lanceTable = "`${catalogName}`.`doris`.`predicate_pushdown`"

    sql "SWITCH internal"
    sql "USE `${internalDb}`"
    sql "DROP TABLE IF EXISTS `${buildTable}`"
    sql "DROP CATALOG IF EXISTS `${catalogName}`"

    try {
        sql """
            CREATE TABLE `${buildTable}` (
                id BIGINT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            )
        """
        sql "INSERT INTO `${buildTable}` VALUES (4), (7), (9), (11)"

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
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET runtime_filter_mode = 'GLOBAL'"
        sql "SET runtime_filter_type = 'IN'"
        sql "SET runtime_filter_wait_infinitely = true"
        sql "SET enable_runtime_filter_prune = false"

        String query = """
            SELECT /*+ leading(l broadcast b) */ l.row_id, l.int64_value
            FROM ${lanceTable} l
            INNER JOIN `internal`.`${internalDb}`.`${buildTable}` b
                ON l.row_id = b.id
            WHERE l.int64_value >= 10
            ORDER BY l.row_id
        """

        // The regular WHERE predicate is converted to the primary Substrait filter. The join
        // produces an IN runtime filter on row_id, which must be attached to the Lance scan.
        explain {
            sql "verbose ${query}"
            check { explainString ->
                assertTrue(explainString.contains("VLANCE_SCAN_NODE"))
                assertTrue(explainString.contains("lancePushdownPredicate="))
                assertTrue(explainString.contains("int64_value"))
                assertTrue(explainString.contains("runtime filters: RF"))
                assertTrue(explainString.contains("-> row_id"))
                return true
            }
        }

        // Build-side IDs are 4, 7, 9 and 11. The static Lance predicate keeps only rows whose
        // int64_value is at least 10, so the intersection is exactly rows 7 and 9. In particular,
        // this catches an implementation that replaces the Substrait filter with the later RF.
        qt_runtime_filter_and_substrait "${query}"

        // Compare Doris's dedicated two-phase row-id fetch with the equivalent SQL formulation:
        // first produce narrow ANN candidates, then broadcast them and fetch payload columns from
        // a normal Lance scan. Both queries must return the same ordered rows and distances.
        String vectorTable = "${catalogName}.doris.vs_ivf_pq_f32"
        String qualifiedVectorTable = "`${catalogName}`.`doris`.`vs_ivf_pq_f32`"
        String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
        String vectorSearch = """vector_search(
                "table"="${vectorTable}",
                "column"="embedding",
                "query_vector"="${headQuery}",
                "top_k"="5",
                "metric"="l2",
                "nprobes"="4",
                "refine_factor"="10",
                "use_index"="true")"""
        String twoPhaseQuery = """
            SELECT row_id, category, label, _distance
            FROM ${vectorSearch}
            ORDER BY _distance, row_id
        """

        sql "SET topn_lazy_materialization_threshold = 1024"
        explain {
            sql "verbose ${twoPhaseQuery}"
            contains "VMaterializeNode"
            contains "__DORIS_GLOBAL_ROWID_COL__vector_search"
        }
        qt_two_phase_vector_search "${twoPhaseQuery}"

        String explicitJoinQuery = """
            WITH candidates AS (
                SELECT row_id, _distance
                FROM ${vectorSearch}
                ORDER BY _distance, row_id
                LIMIT 5
            )
            SELECT w.row_id, w.category, w.label, c._distance
            FROM ${qualifiedVectorTable} w
            JOIN [broadcast] candidates c ON w.row_id = c.row_id
            ORDER BY c._distance, c.row_id
        """

        sql "SET topn_lazy_materialization_threshold = -1"
        sql "SET disable_join_reorder = true"
        explain {
            sql "verbose ${explicitJoinQuery}"
            contains "VHASH JOIN"
            contains "JOIN(BROADCAST)"
            contains "runtime filters: RF"
            contains "-> row_id"
            notContains "VMaterializeNode"
        }
        qt_explicit_join_vector_search "${explicitJoinQuery}"
    } finally {
        // sql "SWITCH internal"
        // sql "USE `${internalDb}`"
        // sql "DROP TABLE IF EXISTS `${buildTable}`"
        // sql "DROP CATALOG IF EXISTS `${catalogName}`"
    }
}
