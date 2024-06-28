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

suite("test_get_stream_load_state", "p0") {
    def tableName = "test_get_stream_load_state"
    String db = context.config.getDbNameByFile(context.file)
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL DEFAULT "1",
            `k2` bigint(20) NULL ,
            `v1` tinyint(4) NULL,
            `v2` tinyint(4) NULL,
            `v3` tinyint(4) NULL,
            `v4` DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def label = UUID.randomUUID().toString().replaceAll("-", "")
    streamLoad {
        table "${tableName}"

        set 'column_separator', '|'
        set 'columns', 'k2, v1, v2, v3'
        set 'strict_mode', 'true'
        set 'label', "${label}"

        file 'test_default_value.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    def command = "curl --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword} http://${context.config.feHttpAddress}/api/${db}/get_load_state?label=${label}"
    log.info("test_get_stream_load_state: ${command}")
    def process = command.execute()
    code = process.waitFor()
    out = process.text
    json = parseJson(out)
    log.info("test_get_stream_load_state: ${out}".toString())
    assertEquals("success", json.msg.toLowerCase())
    assertEquals("VISIBLE", json.data)

    def label1 = UUID.randomUUID().toString().replaceAll("-", "")
    command = "curl --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword} http://${context.config.feHttpAddress}/api/${db}/get_load_state?label=${label1}"
    log.info("test_get_stream_load_state: ${command}")
    process = command.execute()
    code = process.waitFor()
    out = process.text
    json = parseJson(out)
    log.info("test_get_stream_load_state: ${out}".toString())
}