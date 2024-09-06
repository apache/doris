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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_ignore_above_in_index", "p0") {
    def tableName = "test_ignore_above_in_index"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}(
            `id`int(11)NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("ignore_above"="9") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // ignore_above = 9, insert string length = 10
    sql "insert into ${tableName} values (20, '1234567890');"
    sql "insert into ${tableName} values (20, '1234567890');"
    sql "insert into ${tableName} values (20, '1234567890');"
    qt_sql "select count() from ${tableName} where c = '1234567890';"

    def tableName2 = "test_ignore_above_in_index2"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
        CREATE TABLE ${tableName2} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` int NULL COMMENT "",
          `size` int NULL COMMENT "",
          INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("ignore_above"="5") COMMENT '',
          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
          INDEX status_idx (`status`) USING INVERTED COMMENT '',
          INDEX size_idx (`size`) USING INVERTED COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    // load the json data
    streamLoad {
        table "${tableName2}"
        
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
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }
    sql """ set enable_common_expr_pushdown = true; """
    qt_sql "select count() from ${tableName2} where clientip > '17.0';"
    qt_sql "select count() from ${tableName2} where clientip > '17.0' or status = 200;"
}
