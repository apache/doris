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

suite("test_pinyin_phrase", "p0") {
    def indexTbName = "test_pinyin_phrase_table"
    
    sql """ set enable_match_without_inverted_index = false """

    // Create pinyin analyzer
    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS cp_test
        PROPERTIES (
            "tokenizer" = "pinyin"
        );
    """

    sql """ select sleep(10) """

    // Drop table if exists
    sql "DROP TABLE IF EXISTS ${indexTbName}"
    
    // Create table with inverted index using pinyin analyzer
    sql """
        CREATE TABLE ${indexTbName} (
            `a` bigint NOT NULL AUTO_INCREMENT(1),
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES(
                "analyzer" = "cp_test", 
                "support_phrase" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert test data
    sql """ INSERT INTO ${indexTbName} VALUES (1, "合作高峰论坛"); """
    sql """ INSERT INTO ${indexTbName} VALUES (2, "合作伙伴关系"); """
    sql """ INSERT INTO ${indexTbName} VALUES (3, "高峰论坛会议"); """
    sql """ INSERT INTO ${indexTbName} VALUES (4, "合作"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql_1 """ SELECT a, ch FROM ${indexTbName} WHERE search('ch:"合作"') ORDER BY a; """
        qt_sql_2 """ SELECT a, ch FROM ${indexTbName} WHERE search('ch:"高峰论坛"') ORDER BY a; """
        qt_sql_3 """ SELECT a, ch FROM ${indexTbName} WHERE search('ch:"合作高峰"') ORDER BY a; """
        qt_sql_4 """ SELECT a, ch FROM ${indexTbName} WHERE search('ch:"伙伴"') ORDER BY a; """
        qt_sql_5 """ SELECT a, ch FROM ${indexTbName} WHERE search('ch:"不存在"') ORDER BY a; """

    } finally {
        // Cleanup
        sql "DROP TABLE IF EXISTS ${indexTbName}"
    }
}
