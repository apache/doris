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

suite("test_or_not_match", "p0") {
    def tableName = "test_or_not_match"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
      CREATE TABLE ${tableName} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """
    // load the json data
    streamLoad {
        table "${tableName}"
        
        // set http request header params
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
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }
    for (int i = 0; i < 10; i++) {
        sql "select request from ${tableName} where request like '1.0' or not request MATCH 'GETA';"
    }

    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql """ set enable_common_expr_pushdown = true """

    qt_sql "select request from ${tableName} where request like '1.0' or not request MATCH 'GETA' order by request limit 2;"
}
