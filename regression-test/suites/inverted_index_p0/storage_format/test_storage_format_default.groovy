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

suite("test_storage_format_default", "p0") {
    def testTable = "httplogs_dup_default"

    def create_httplogs_dup_table = { tableName ->
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `@timestamp` int(11) NULL,
              `clientip` varchar(20) NULL,
              `request` string NULL,
              `status` int(11) NULL,
              `size` int(11) NULL,
              INDEX size_idx (`size`) USING INVERTED COMMENT '',
              INDEX status_idx (`status`) USING INVERTED COMMENT '',
              INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
              INDEX request_idx (`request`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "english", "lower_case" = "true") COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`@timestamp`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 2
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "disable_auto_compaction" = "true"
            );
        """
    }

    def load_httplogs_data = { tableName, label, readFlag, formatFlag, fileName, ignoreFailure = false,
                               expectedSuccRows = -1 ->
        streamLoad {
            table "${tableName}"

            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', readFlag
            set 'format', formatFlag
            file fileName
            time 10000
            if (expectedSuccRows >= 0) {
                set 'max_filter_ratio', '1'
            }

            check { result, exception, startTime, endTime ->
                if (ignoreFailure && expectedSuccRows < 0) { return }
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                if (expectedSuccRows >= 0) {
                    assertEquals(json.NumberLoadedRows, expectedSuccRows)
                } else {
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_httplogs_dup_table.call(testTable)

        def showCreate = sql """ SHOW CREATE TABLE ${testTable} """
        assertTrue(showCreate.size() > 0)
        assertTrue(showCreate[0][1].contains("\"inverted_index_storage_format\" = \"V3\""))

        load_httplogs_data.call(testTable, 'label', 'true', 'json', 'documents-1000.json')
        sql "sync"

        qt_sql("select COUNT(*) from ${testTable} where request match 'images'")
    } finally {
        sql("DROP TABLE IF EXISTS ${testTable}")
    }
}