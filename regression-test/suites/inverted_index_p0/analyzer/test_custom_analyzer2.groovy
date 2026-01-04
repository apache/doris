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

suite("test_custom_analyzer2", "p0") {
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS empty_analyzer
        PROPERTIES
        (
            "tokenizer" = "empty",
            "char_filter" = "empty",
            "token_filter" = "empty"
        );
    """

    sql """ select sleep(10) """

    qt_tokenize_sql """ select tokenize("empty analyzer", '"analyzer"="empty_analyzer"'); """

    sql "DROP TABLE IF EXISTS test_custom_analyzer2_1"
    sql """
        CREATE TABLE test_custom_analyzer2_1 (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("built_in_analyzer" = "standard", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    try {
        sql """ insert into test_custom_analyzer2_1 values(1, "中国人民"); """
        sql """ insert into test_custom_analyzer2_1 values(2, "美国人民"); """
        
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select count(1) from test_custom_analyzer2_1 where ch match '中'; """
        qt_sql """ select count(1) from test_custom_analyzer2_1 where ch match '国'; """
        qt_sql """ select count(1) from test_custom_analyzer2_1 where ch match '人'; """
        qt_sql """ select count(1) from test_custom_analyzer2_1 where ch match '民'; """
    } finally {
    }
}