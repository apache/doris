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

// Analyzed indexes on variant subcolumns must not write dense BM25 norms (.nrm, one byte per row),
// while analyzed indexes on ordinary columns still do. BM25 scoring keeps working on both.
suite("test_variant_subcolumn_index_norms", "p0") {
    if (isCloudMode()) {
        return
    }

    sql """ set enable_segment_limit_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS test_variant_subcolumn_index_norms"
    sql """
        CREATE TABLE test_variant_subcolumn_index_norms (
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
            "inverted_index_storage_format" = "V2"
        )
    """
    sql """ insert into test_variant_subcolumn_index_norms values
            (1, 'alpha database server', parse_to_variant('{"s_host":"alpha database server"}')),
            (2, 'beta server cluster', parse_to_variant('{"s_host":"beta server cluster", "s_note":"alpha"}')),
            (3, 'alpha', parse_to_variant('{"s_note":"alpha alpha beta"}')),
            (4, 'gamma', parse_to_variant('{"other":"alpha"}'))
    """
    sql " sync "

    // scores are printed so that a NaN (which compares greater than 0) cannot slip through
    order_qt_variant_subcolumn_score """
        select id, round(score(), 4)
        from test_variant_subcolumn_index_norms
        where cast(v["s_note"] as string) match_phrase "alpha"
        order by score() desc
        limit 10
    """
    order_qt_plain_column_score """
        select id, round(score(), 4)
        from test_variant_subcolumn_index_norms
        where content match_phrase "alpha"
        order by score() desc
        limit 10
    """

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
    def tablet = sql_return_maparray("show tablets from test_variant_subcolumn_index_norms")[0]
    def (code, out, err) = http_client("GET", String.format(
            "http://%s:%s/api/show_nested_index_file?tablet_id=%s",
            backendIdToIp.get(tablet.BackendId), backendIdToHttpPort.get(tablet.BackendId),
            tablet.TabletId))
    logger.info("show_nested_index_file code=${code}, out=${out}, err=${err}")
    assertEquals(0, code)

    def subcolumnIndexes = []
    def plainIndexes = []
    for (def rowset in parseJson(out.trim()).rowsets) {
        for (def segment in rowset.segments) {
            for (def index in segment.indices) {
                def hasNorms = index.files.any { file -> file.name.endsWith(".nrm") }
                if (index.index_suffix.isEmpty()) {
                    plainIndexes.add(hasNorms)
                } else {
                    subcolumnIndexes.add(hasNorms)
                }
            }
        }
    }
    logger.info("norms of plain indexes: ${plainIndexes}, subcolumn indexes: ${subcolumnIndexes}")
    // idx_content on the single segment writes norms; idx_v_s on s_host and s_note does not
    assertEquals([true], plainIndexes)
    assertEquals([false, false], subcolumnIndexes)
}
