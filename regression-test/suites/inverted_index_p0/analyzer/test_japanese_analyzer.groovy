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

    def get_be_config = { backend_id, key ->
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id),
                                              backendId_to_backendHttpPort.get(backend_id))
        assertEquals(0, code)
        for (Object ele in (List) parseJson(out.trim())) {
            if (((List<String>) ele)[0] == key) {
                return ((List<String>) ele)[2]
            }
        }
        assertTrue(false, "BE config not found: " + key)
    }
    def set_be_config = { key, value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id),
                             backendId_to_backendHttpPort.get(backend_id), key, value)
            assertEquals(0, code)
        }
    }

    def originalEnable = [:]
    for (String backend_id : backendId_to_backendIP.keySet()) {
        originalEnable[backend_id] = get_be_config(backend_id, "enable_kuromoji_analyzer")
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

        // Search mode decomposes the compound so its Tokyo component matches row 1.
        def tokyo = sql """ SELECT id FROM ${tableName} WHERE content MATCH '東京' ORDER BY id """
        assertEquals(1, tokyo.size())
        assertEquals(1, tokyo[0][0])

        // The same decomposition preserves matches for the complete compound.
        def compound = sql """ SELECT id FROM ${tableName} WHERE content MATCH '東京都' ORDER BY id """
        assertEquals(1, compound.size())
        assertEquals(1, compound[0][0])

        // The sushi noun is a separate morpheme in row 2.
        def sushi = sql """ SELECT id FROM ${tableName} WHERE content MATCH '寿司' ORDER BY id """
        assertEquals(1, sushi.size())
        assertEquals(2, sushi[0][0])

        // Base-form normalization lets the verb lemma match row 1.
        def live = sql """ SELECT id FROM ${tableName} WHERE content MATCH '住む' ORDER BY id """
        assertEquals(1, live.size())
        assertEquals(1, live[0][0])

        // Directly show search mode emits the Tokyo part of the Tokyo Metropolis compound.
        def tokens = sql """SELECT TOKENIZE('東京都', '"parser"="kuromoji","parser_mode"="search"');"""
        def tokenStr = tokens[0][0].toString()
        assertTrue(tokenStr.contains('"token": "東京"'))
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
        for (String backend_id : originalEnable.keySet()) {
            update_be_config(backendId_to_backendIP.get(backend_id),
                             backendId_to_backendHttpPort.get(backend_id),
                             "enable_kuromoji_analyzer", originalEnable[backend_id])
        }
    }
}
