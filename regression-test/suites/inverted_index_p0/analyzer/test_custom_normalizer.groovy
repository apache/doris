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

import java.sql.SQLException

suite("test_custom_normalizer", "p0") {
    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS nfkc_cf_token_filter
        PROPERTIES
        (
            "type" = "icu_normalizer",
            "name" = "nfkc_cf"
        );
    """

    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS nfkc_cf_alpha_token_filter
        PROPERTIES
        (
            "type" = "icu_normalizer",
            "name" = "nfkc_cf",
            "unicode_set_filter" = "[A-Za-z]"
        );
    """

    sql """
        CREATE INVERTED INDEX NORMALIZER IF NOT EXISTS nfkc_cf_normalizer
        PROPERTIES
        (
            "token_filter" = "nfkc_cf_token_filter"
        );
    """

    sql """
        CREATE INVERTED INDEX NORMALIZER IF NOT EXISTS nfkc_cf_alpha_normalizer
        PROPERTIES
        (
            "token_filter" = "nfkc_cf_alpha_token_filter"
        );
    """

    sql """ select sleep(10) """

    qt_tokenize_normalizer_nfc_1 """
        select tokenize(
            cast(unhex('43616665CC81') as string),
            '"normalizer"="nfkc_cf_normalizer"'
        );
    """

    qt_tokenize_normalizer_nfc_2 """
        select tokenize(
            cast(unhex('436166C3A9') as string),
            '"normalizer"="nfkc_cf_normalizer"'
        );
    """

    qt_tokenize_normalizer_alpha """
        select tokenize(
            'Café 123',
            '"normalizer"="nfkc_cf_alpha_normalizer"'
        );
    """

    sql "DROP TABLE IF EXISTS test_custom_normalizer_tbl"
    sql """
        CREATE TABLE test_custom_normalizer_tbl (
            `id` bigint NOT NULL,
            `content` text NULL,
            INDEX idx_content (`content`) USING INVERTED
            PROPERTIES("normalizer" = "nfkc_cf_normalizer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO test_custom_normalizer_tbl VALUES
          (1, cast(unhex('43616665CC81') as string)), -- Cafe + 组合重音
          (2, cast(unhex('436166C3A9')   as string)), -- Caf + 预组合 é
          (3, 'CAFÉ');
    """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql_match_cafe """
            SELECT id, content
            FROM test_custom_normalizer_tbl
            WHERE content MATCH 'café'
            ORDER BY id;
        """
    } finally {
    }

    test {
        sql """
            CREATE TABLE test_custom_normalizer_invalid (
                `id` int,
                `content` text,
                INDEX idx (`content`) USING INVERTED
                PROPERTIES(
                    "analyzer" = "basic",
                    "normalizer" = "nfkc_cf_normalizer"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES("replication_allocation" = "tag.location.default: 1");
        """
        exception "Cannot specify more than one of 'analyzer', 'parser', or 'normalizer'"
    }
}