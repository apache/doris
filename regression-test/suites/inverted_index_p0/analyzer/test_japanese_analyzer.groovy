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

suite("test_japanese_analyzer", "p0") {
    def tableName = "test_japanese_analyzer"

    sql "DROP TABLE IF EXISTS ${tableName}"
    try {
        sql """
          CREATE TABLE ${tableName} (
            `id` int(11) NULL COMMENT "",
            `content` text NULL COMMENT "",
            INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "kuromoji", "parser_mode" = "search") COMMENT '',
          ) ENGINE=OLAP
          DUPLICATE KEY(`id`)
          COMMENT "OLAP"
          DISTRIBUTED BY RANDOM BUCKETS 1
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
          );
        """

        sql """ INSERT INTO ${tableName} VALUES (1, "東京都に住んでいます"); """
        sql """ INSERT INTO ${tableName} VALUES (2, "私は寿司が好きです"); """
        sql """ INSERT INTO ${tableName} VALUES (3, "Apache Doris は高速です"); """
        sql "sync"

        // P0 stub is CJK-unigram: every CJK codepoint is its own token,
        // so a single-character MATCH must hit the row containing that character.
        qt_sql_tokyo """ SELECT id FROM ${tableName} WHERE content MATCH '東' ORDER BY id; """
        qt_sql_sushi """ SELECT id FROM ${tableName} WHERE content MATCH '寿' ORDER BY id; """

        // Verify the TOKENIZE function dispatches to the kuromoji parser.
        // Quoting follows the literal-string form proven in test_tokenize.groovy:97 —
        // property string uses double-quoted keys/values inside a single-quoted outer string.
        qt_sql_tokenize """SELECT TOKENIZE('東京都', '"parser"="kuromoji"');"""
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
