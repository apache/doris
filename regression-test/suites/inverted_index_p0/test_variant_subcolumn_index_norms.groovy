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

// An analyzed index writes dense BM25 norms (.nrm, one byte per segment row), on a variant path as
// on any other column, and "norms" = "false" drops them. Norms on a variant path cost rows * paths
// bytes, so inverted_index_skip_norms_for_variant leaves them out there whatever the property says.
// This covers both an index declared with a field_pattern and a whole-column
// index on a VARIANT column, whose per-subcolumn copies inherit the properties of the index they
// come from. BM25 scoring keeps working with and without norms.
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
                't_*' : text,
                PROPERTIES("variant_max_subcolumns_count"="0")
            >,
            vd variant<
                'a_*' : text,
                PROPERTIES("variant_max_subcolumns_count"="0")
            >,
            vn variant<
                'b_*' : text,
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
            ),
            INDEX idx_v_t (v) USING INVERTED PROPERTIES(
                "parser"="english",
                "support_phrase"="true",
                "field_pattern"="t_*",
                "norms"="false"
            ),
            INDEX idx_vd (vd) USING INVERTED PROPERTIES(
                "parser"="english",
                "support_phrase"="true"
            ),
            INDEX idx_vn (vn) USING INVERTED PROPERTIES(
                "parser"="english",
                "support_phrase"="true",
                "norms"="false"
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
            (1, 'alpha database server', parse_to_variant('{"s_host":"alpha database server"}'),
                parse_to_variant('{"a_host":"alpha database server"}'),
                parse_to_variant('{"b_host":"alpha database server"}')),
            (2, 'beta server cluster', parse_to_variant('{"s_host":"beta server cluster", "s_note":"alpha", "t_note":"alpha"}'),
                parse_to_variant('{"a_host":"beta server cluster"}'),
                parse_to_variant('{"b_host":"beta server cluster"}')),
            (3, 'alpha', parse_to_variant('{"s_note":"alpha alpha beta", "t_note":"alpha beta"}'),
                parse_to_variant('{"a_host":"alpha"}'),
                parse_to_variant('{"b_host":"alpha"}')),
            (4, 'gamma', parse_to_variant('{"other":"alpha"}'),
                parse_to_variant('{"other":"alpha"}'),
                parse_to_variant('{"other":"alpha"}'))
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
    order_qt_variant_subcolumn_score_no_norms """
        select id, round(score(), 4)
        from test_variant_subcolumn_index_norms
        where cast(v["t_note"] as string) match_phrase "alpha"
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
    def normsBySuffixOf = { tableName ->
        def tablet = sql_return_maparray("show tablets from ${tableName}")[0]
        def (code, out, err) = http_client("GET", String.format(
                "http://%s:%s/api/show_nested_index_file?tablet_id=%s",
                backendIdToIp.get(tablet.BackendId), backendIdToHttpPort.get(tablet.BackendId),
                tablet.TabletId))
        logger.info("show_nested_index_file of ${tableName} code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        def norms = [:]
        for (def rowset in parseJson(out.trim()).rowsets) {
            for (def segment in rowset.segments) {
                for (def index in segment.indices) {
                    norms[index.index_suffix] = index.files.any { file -> file.name.endsWith(".nrm") }
                }
            }
        }
        logger.info("norms by index suffix of ${tableName}: ${norms}")
        return norms
    }
    // the suffix is the escaped variant path, e.g. v%2Es%5Fhost for v.s_host
    def normsOf = { norms, path ->
        norms.find { suffix, hasNorms ->
            suffix.replace("%2E", ".").replace("%5F", "_").contains(path)
        }?.value
    }

    def normsBySuffix = normsBySuffixOf("test_variant_subcolumn_index_norms")
    // an analyzed index writes norms wherever it sits, and "norms" = "false" drops them
    assertEquals(true, normsBySuffix[""])
    assertEquals(true, normsOf(normsBySuffix, "s_host"))
    assertEquals(true, normsOf(normsBySuffix, "s_note"))
    assertEquals(false, normsOf(normsBySuffix, "t_note"))
    // a whole-column index on a VARIANT column has no suffix of its own, but every subcolumn copy
    // inherits its properties: idx_vd keeps norms, idx_vn drops them because it asks to
    assertEquals(true, normsOf(normsBySuffix, "a_host"))
    assertEquals(false, normsOf(normsBySuffix, "b_host"))

    // the skip is a dynamic BE config: with it turned on, every index on a variant path leaves norms
    // out, even one that asks for them, while an ordinary column index is untouched
    setBeConfigTemporary([inverted_index_skip_norms_for_variant: true]) {
        sql "DROP TABLE IF EXISTS test_variant_subcolumn_index_norms_config"
        sql """
            CREATE TABLE test_variant_subcolumn_index_norms_config (
                id INT,
                content TEXT,
                v variant<
                    's_*' : text,
                    PROPERTIES("variant_max_subcolumns_count"="0")
                >,
                vf variant<
                    'c_*' : text,
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
                ),
                INDEX idx_vf (vf) USING INVERTED PROPERTIES(
                    "parser"="english",
                    "support_phrase"="true",
                    "norms"="true"
                )
            ) ENGINE=OLAP DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V2"
            )
        """
        sql """ insert into test_variant_subcolumn_index_norms_config values
                (1, 'alpha database server', parse_to_variant('{"s_host":"alpha database server"}'),
                    parse_to_variant('{"c_host":"alpha database server"}')),
                (2, 'beta server cluster', parse_to_variant('{"s_host":"beta server cluster"}'),
                    parse_to_variant('{"c_host":"beta server cluster"}'))
        """
        sql " sync "

        def configNorms = normsBySuffixOf("test_variant_subcolumn_index_norms_config")
        assertEquals(false, normsOf(configNorms, "s_host"))
        assertEquals(false, normsOf(configNorms, "c_host"))
        assertEquals(true, configNorms[""])
    }
}
