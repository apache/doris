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

suite("test_custom_analyzer1", "p0") {
    def indexTbName1 = "test_custom_analyzer1"

    sql """
        CREATE INVERTED INDEX TOKEN_FILTER IF NOT EXISTS word_splitter_all
        PROPERTIES
        (
            "type" = "word_delimiter",
            "generate_word_parts" = "true",
            "generate_number_parts" = "true",
            "split_on_case_change" = "true",
            "split_on_numerics" = "true",
            "stem_english_possessive" = "true"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS custom_standard_analyzer
        PROPERTIES
        (
        "tokenizer" = "standard",
        "token_filter" = "asciifolding, word_splitter_all, lowercase"
        );
    """

    sql """ select sleep(10) """
     
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
        CREATE TABLE ${indexTbName1} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("support_phrase" = "true", "analyzer" = "custom_standard_analyzer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${indexTbName1} values(1, "A two-hour programme which included many forms of [[jazz]] from classic to Latin as well as a mix of jazz from the younger players of the day."); """
    sql """ insert into ${indexTbName1} values(2, " with off-peak shows introducing more commercial breaks into their output, before the concept was dropped altogether in mid-2006."); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select * from ${indexTbName1} where ch match 'with'; """
        qt_sql """ select * from ${indexTbName1} where ch match 'the'; """
    } finally {
    }
}