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

// SNII keeps BM25 norms for the non-NULL rows only when that form is smaller. The same rows are
// written once with dense norms forced and once with the writer's own choice: both tables return
// the same rows and scores, and the second keeps fewer norm bytes.
suite("test_storage_format_snii_sparse_norms", "p0,nonConcurrent") {
    String dbName = context.config.getDbNameByFile(context.file)
    sql """ set enable_match_without_inverted_index = false """
    sql """ set debug_variant_enable_typed_paths_to_sparse = false """
    sql """ set debug_variant_enable_doc_mode = false """

    def createAndLoad = { String tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                content TEXT,
                v variant<
                    's_*' : text,
                    PROPERTIES("variant_max_subcolumns_count"="0")
                >,
                INDEX idx_content (content) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true"
                ),
                INDEX idx_v_s (v) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true",
                    "field_pattern"="s_*"
                )
            ) ENGINE=OLAP DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            )
        """
        // One row in ten holds text of one to four tokens; the other rows are NULL on both indexes.
        sql """
            INSERT INTO ${tableName}
            SELECT number,
                   if(number % 10 = 0, concat('alpha', repeat(' beta', (number DIV 10) % 4)), NULL),
                   if(number % 10 = 0,
                      parse_to_variant(concat('{"s_note":"alpha',
                                              repeat(' beta', (number DIV 10) % 4), '"}')),
                      parse_to_variant('{"other":"gamma"}'))
            FROM numbers("number" = "400")
        """
        sql " sync "
    }

    def denseNormsPoint = "LogicalIndexWriter::build_norms_section.dense_norms"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(denseNormsPoint)
        createAndLoad("test_snii_sparse_norms_dense")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(denseNormsPoint)
    }
    createAndLoad("test_snii_sparse_norms_chosen")

    // the norms move the scores (rows hold one to four tokens), and both forms give the same ones
    order_qt_dense_content_score """
        select id, round(score(), 4) from test_snii_sparse_norms_dense
        where content match_any "beta" order by score() desc limit 100
    """
    order_qt_chosen_content_score """
        select id, round(score(), 4) from test_snii_sparse_norms_chosen
        where content match_any "beta" order by score() desc limit 100
    """
    order_qt_dense_variant_score """
        select id, round(score(), 4) from test_snii_sparse_norms_dense
        where cast(v["s_note"] as string) match_any "beta" order by score() desc limit 100
    """
    order_qt_chosen_variant_score """
        select id, round(score(), 4) from test_snii_sparse_norms_chosen
        where cast(v["s_note"] as string) match_any "beta" order by score() desc limit 100
    """
    order_qt_chosen_phrase """
        select id from test_snii_sparse_norms_chosen where content match_phrase "alpha beta"
    """

    // the norms are counted in stats_bytes of the index that owns them
    def statsBytes = { String tableName ->
        return """
            SELECT index_name, SUM(stats_bytes) AS stats_bytes
            FROM index_disk_usage("database" = "${dbName}", "table" = "${tableName}")
            WHERE structure = 'TERM'
            GROUP BY index_name
        """
    }
    def sizes = sql """
        SELECT d.index_name, d.stats_bytes, c.stats_bytes
        FROM (${statsBytes("test_snii_sparse_norms_dense")}) d
        JOIN (${statsBytes("test_snii_sparse_norms_chosen")}) c ON d.index_name = c.index_name
        ORDER BY d.index_name
    """
    logger.info("SNII stats bytes per index (dense forced, writer's choice): ${sizes}")
    order_qt_chosen_norms_are_smaller """
        SELECT d.index_name, c.stats_bytes < d.stats_bytes
        FROM (${statsBytes("test_snii_sparse_norms_dense")}) d
        JOIN (${statsBytes("test_snii_sparse_norms_chosen")}) c ON d.index_name = c.index_name
    """
}
