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

suite("test_duplicated_load_id", "nonConcurrent") {

    def create_httplogs_dup_table = {testTablex ->
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 3
                        PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1"
                        );
                        """
    }

    def testTable = "httplogs"
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_httplogs_dup_table.call(testTable)

        try {
            GetDebugPoint().enableDebugPointForAllBEs("NewLoadStreamMgr.test_duplicated_load_id")
            streamLoad {
                table "${testTable}"
                // set http request header params
                set 'label', "test_duplicated_load_id_" + UUID.randomUUID().toString()
                set 'read_json_by_line', 'true'
                set 'format', 'json'
                file 'documents-1000.json' // import json file
                time 10000 // limit inflight 10s
                // if declared a check callback, the default check condition will ignore.
                // So you must check all condition
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.toLowerCase().contains("already exist"))
                }
            }

            qt_sql "SELECT COUNT(*) FROM ${testTable}"

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("NewLoadStreamMgr.test_duplicated_load_id")
        }

    } finally {
        sql "DROP TABLE IF EXISTS ${testTable}"
    }
}
