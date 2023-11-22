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

suite("test_big_kv_map", "p2") {
    def testTable = "test_big_kv_map"

    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
                CREATE TABLE IF NOT EXISTS ${testTable} (
                    `id` bigint(20) NULL,
                    `actor` MAP<text,text> NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`id`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "in_memory" = "false",
                    "storage_format" = "V2",
                    "light_schema_change" = "true",
                    "disable_auto_compaction" = "false"
                );
        """

    def dataFile = """${getS3Url() + '/regression/datatypes/' + testTable}.csv.gz"""

    try {
        // load data
        streamLoad {
            table testTable
            file dataFile
            time 72000

            set 'compress_type', 'GZ'
            set 'timeout', '72000'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(100, json.NumberTotalRows)
                assertEquals(100, json.NumberLoadedRows)
            }
        }
        sql "sync"
        qt_select_total "SELECT count(*) FROM ${testTable}"
        qt_select_map "SELECT count(actor) FROM ${testTable}"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
