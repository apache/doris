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

suite("test_json_disable_simdjson_reader", "p0") {

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }
  
    try {
        set_be_param.call("enable_simdjson_reader", "false")

        // define a sql table
        def testTable = "disable_simdjson_reader"
        def dataFile = "disable_simdjson_reader.json"

        sql "DROP TABLE IF EXISTS ${testTable}"

        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                id INT NOT NULL,
                city VARCHAR(64) NULL,
                code INT NULL
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 5
            PROPERTIES("replication_num" = "1");
        """

        // load with json_path
        streamLoad {
            table testTable
            // set http request header params
            file dataFile
            time 10000
            set 'strict_mode', 'true'
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'jsonpaths', '[\"$.id\", \"$.city.name\", \"$.code\"]'
            set 'merge_type', 'merge'
            set 'delete', 'id<103'
            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(6, json.NumberTotalRows)
                assertEquals(6, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertTrue(json.LoadBytes > 0)
            }
        }

        qt_select "SELECT * FROM ${testTable} ORDER BY id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
        set_be_param.call("enable_simdjson_reader", "true")
    }
}

