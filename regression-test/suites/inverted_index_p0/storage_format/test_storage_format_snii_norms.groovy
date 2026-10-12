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

// SNII follows the same norms policy as the CLucene formats: an analyzed index writes BM25 norms
// unless "norms" = "false", and inverted_index_skip_norms_for_variant drops them for every index on
// a variant path whatever the property says. BM25 scoring needs norms, so score() on an index
// without them fails, while MATCH filtering keeps working. Norms are not visible from outside a
// SNII file, so this suite reads them off the queries: with norms, rows 1 and 2 match "alpha" once
// each and the three-token row 1 scores lower than the one-token row 2; without norms, score()
// is refused.
// It flips a BE config, so it must not share the cluster with other suites.
suite("test_storage_format_snii_norms", "p0,nonConcurrent") {
    sql """ set enable_match_without_inverted_index = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS test_storage_format_snii_norms"
    sql """
        CREATE TABLE test_storage_format_snii_norms (
            id INT,
            content TEXT,
            v variant<
                's_*' : text,
                't_*' : text,
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
            "inverted_index_storage_format" = "SNII"
        )
    """
    sql """ insert into test_storage_format_snii_norms values
            (1, 'alpha database server',
                parse_to_variant('{"s_note":"alpha database server", "t_note":"alpha database server"}'),
                parse_to_variant('{"b_note":"alpha database server"}')),
            (2, 'alpha',
                parse_to_variant('{"s_note":"alpha", "t_note":"alpha"}'),
                parse_to_variant('{"b_note":"alpha"}')),
            (3, 'gamma', parse_to_variant('{"other":"gamma"}'), parse_to_variant('{"other":"gamma"}'))
    """
    sql " sync "

    // an ordinary column and a field_pattern index keep norms by default: row 1 scores lower
    order_qt_snii_plain_column_score """
        select id, round(score(), 4) from test_storage_format_snii_norms
        where content match_any "alpha" order by score() desc limit 10
    """
    order_qt_snii_field_pattern_score """
        select id, round(score(), 4) from test_storage_format_snii_norms
        where cast(v["s_note"] as string) match_any "alpha" order by score() desc limit 10
    """

    // "norms" = "false" drops them, on a field_pattern index and on the copies a whole-column
    // index hands to its subcolumns: MATCH still filters, score() is refused
    order_qt_snii_field_pattern_no_norms_match """
        select id from test_storage_format_snii_norms
        where cast(v["t_note"] as string) match_any "alpha"
    """
    test {
        sql """
            select id, score() from test_storage_format_snii_norms
            where cast(v["t_note"] as string) match_any "alpha" order by score() desc limit 10
        """
        exception "written without"
    }
    order_qt_snii_whole_column_no_norms_match """
        select id from test_storage_format_snii_norms
        where cast(vn["b_note"] as string) match_any "alpha"
    """
    test {
        sql """
            select id, score() from test_storage_format_snii_norms
            where cast(vn["b_note"] as string) match_any "alpha" order by score() desc limit 10
        """
        exception "written without"
    }

    // with the config on, every index on a variant path leaves norms out, even one that asks for
    // them, while an ordinary column index keeps them
    setBeConfigTemporary([inverted_index_skip_norms_for_variant: true]) {
        sql "DROP TABLE IF EXISTS test_storage_format_snii_norms_config"
        sql """
            CREATE TABLE test_storage_format_snii_norms_config (
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
                "inverted_index_storage_format" = "SNII"
            )
        """
        sql """ insert into test_storage_format_snii_norms_config values
                (1, 'alpha database server',
                    parse_to_variant('{"s_note":"alpha database server"}'),
                    parse_to_variant('{"c_note":"alpha database server"}')),
                (2, 'alpha', parse_to_variant('{"s_note":"alpha"}'), parse_to_variant('{"c_note":"alpha"}')),
                (3, 'gamma', parse_to_variant('{"other":"gamma"}'), parse_to_variant('{"other":"gamma"}'))
        """
        sql " sync "
    }

    order_qt_snii_config_plain_column_score """
        select id, round(score(), 4) from test_storage_format_snii_norms_config
        where content match_any "alpha" order by score() desc limit 10
    """
    test {
        sql """
            select id, score() from test_storage_format_snii_norms_config
            where cast(v["s_note"] as string) match_any "alpha" order by score() desc limit 10
        """
        exception "written without"
    }
    test {
        sql """
            select id, score() from test_storage_format_snii_norms_config
            where cast(vf["c_note"] as string) match_any "alpha" order by score() desc limit 10
        """
        exception "written without"
    }

    // segments with and without norms side by side, as while the config is being turned on:
    // MATCH still filters, and score() is refused rather than ranking the two kinds differently
    sql "DROP TABLE IF EXISTS test_storage_format_snii_norms_mixed"
    sql """
        CREATE TABLE test_storage_format_snii_norms_mixed (
            id INT,
            v variant<
                's_*' : text,
                PROPERTIES("variant_max_subcolumns_count"="0")
            >,
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
    sql """ insert into test_storage_format_snii_norms_mixed values
            (1, parse_to_variant('{"s_note":"alpha database server"}'))
    """
    setBeConfigTemporary([inverted_index_skip_norms_for_variant: true]) {
        sql """ insert into test_storage_format_snii_norms_mixed values
                (2, parse_to_variant('{"s_note":"alpha"}'))
        """
    }
    sql " sync "

    order_qt_snii_mixed_match """
        select id from test_storage_format_snii_norms_mixed
        where cast(v["s_note"] as string) match_any "alpha"
    """
    test {
        sql """
            select id, score() from test_storage_format_snii_norms_mixed
            where cast(v["s_note"] as string) match_any "alpha" order by score() desc limit 10
        """
        exception "written without"
    }
}
