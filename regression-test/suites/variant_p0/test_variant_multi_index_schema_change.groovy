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

suite("test_variant_multi_index_schema_change", "p0") {

    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set inverted_index_skip_threshold = 0 """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """

    def createVariantTable = { tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `tag` string NULL,
            `var` variant<properties ("variant_max_subcolumns_count" = "10")> NOT NULL,
            INDEX idx_a_d (`var`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
            INDEX idx_a_d_2 (`var`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
	    "inverted_index_storage_format" = "V2"
        )
        """
    }

    def insertBatch = { tableName ->
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'hello', '{"string" : "hello", "array_string" : ["hello"]}'),
                (2, 'world', '{"string" : "world", "array_string" : ["world"]}'),
                (3, 'hello', '{"string" : "hello", "array_string" : ["hello"]}'),
                (4, 'world', '{"string" : "world", "array_string" : ["world"]}'),
                (5, 'hello', '{"string" : "hello", "array_string" : ["hello"]}')
        """
    }

    def loadTable = { tableName ->
        insertBatch(tableName)
        for (int i = 0; i < 10; i++) {
            insertBatch(tableName)
        }
        trigger_and_wait_compaction(tableName, "cumulative")
    }

    def waitSchemaChange = { tableName ->
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
    }

    def assertPhraseCount = { tableName ->
        assertEquals(33, sql(""" SELECT COUNT() FROM ${tableName} WHERE var['string'] MATCH_PHRASE 'hello' """)[0][0])
    }

    def assertNestedIndexFileReady = { tableName ->
        def backendIdToBackendIP = [:]
        def backendIdToBackendHttpPort = [:]
        getBackendIpHttpPort(backendIdToBackendIP, backendIdToBackendHttpPort)
        def tablets = sql_return_maparray(""" SHOW TABLETS FROM ${tableName} """)
        String tabletId = tablets[0].TabletId
        String backendId = tablets[0].BackendId
        String ip = backendIdToBackendIP.get(backendId)
        String port = backendIdToBackendHttpPort.get(backendId)
        check_nested_index_file(ip, port, tabletId, 2, 3, "V2")
    }

    def assertBloomFilterReady = { tableName ->
        assertTrue(sql(""" SHOW CREATE TABLE ${tableName} """)[0][1].contains("\"bloom_filter_columns\" = \"tag\""))
        assertEquals(33, sql(""" SELECT COUNT() FROM ${tableName} WHERE tag = 'hello' """)[0][0])
        assertEquals(33, sql(""" SELECT COUNT() FROM ${tableName} WHERE tag = 'hello' AND var['string'] MATCH_PHRASE 'hello' """)[0][0])
    }

    def tableName = "test_variant_multi_index_schema_change"
    createVariantTable(tableName)
    loadTable(tableName)
    assertPhraseCount(tableName)

    sql """
        ALTER TABLE ${tableName}
        MODIFY COLUMN var variant<properties ("variant_max_subcolumns_count" = "10")> NULL
    """
    waitSchemaChange(tableName)
    assertNestedIndexFileReady(tableName)
    assertPhraseCount(tableName)

    def bloomFilterTable = "test_variant_multi_index_schema_change_bf"
    createVariantTable(bloomFilterTable)
    loadTable(bloomFilterTable)
    assertPhraseCount(bloomFilterTable)

    sql """ ALTER TABLE ${bloomFilterTable} SET ("bloom_filter_columns" = "tag") """
    waitSchemaChange(bloomFilterTable)
    assertNestedIndexFileReady(bloomFilterTable)
    assertBloomFilterReady(bloomFilterTable)
    assertPhraseCount(bloomFilterTable)
}
