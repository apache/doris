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

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    def set_be_config = { key, value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            update_be_config(backendId_to_backendIP.get(backend_id),
                             backendId_to_backendHttpPort.get(backend_id), key, value)
        }
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    // kuromoji is disabled by default; enable it for this test.
    set_be_config("enable_kuromoji_analyzer", "true")
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

        // The kuromoji IPADIC dictionary ships with the package (built by the
        // kuromoji_dict target), so this exercises real morphological analysis --
        // The assertions below cover the real.
        // Search mode decomposes the compound 東京都 into 東京 + 都, so a 東京 query
        // matches row 1 (a single-character 東 query would NOT, unlike a unigram split).
        def tokyo = sql """ SELECT id FROM ${tableName} WHERE content MATCH '東京' ORDER BY id; """
        assertEquals(1, tokyo.size())
        assertTrue(tokyo[0][0] == 1)

        // 寿司 is segmented as its own morpheme in 私は寿司が好きです.
        def sushi = sql """ SELECT id FROM ${tableName} WHERE content MATCH '寿司' ORDER BY id; """
        assertEquals(1, sushi.size())
        assertTrue(sushi[0][0] == 2)

        // Base-form normalization: the conjugated 住ん(でいます) is indexed under its
        // dictionary base form 住む, so a 住む query matches row 1.
        def live = sql """ SELECT id FROM ${tableName} WHERE content MATCH '住む' ORDER BY id; """
        assertEquals(1, live.size())
        assertTrue(live[0][0] == 1)
        def tokens = sql """SELECT TOKENIZE('東京都', '"parser"="kuromoji","parser_mode"="search"');"""
        def tokenStr = tokens[0][0].toString()
        assertTrue(tokenStr.contains('"token": "東京"'))
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
        set_be_config("enable_kuromoji_analyzer", "false")
    }
}
