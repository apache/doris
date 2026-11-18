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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

suite("test_es_date_docvalue_formats", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableEsTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    sql "switch internal"
    sql "drop table if exists es_date_docvalue_expected"
    sql """create table es_date_docvalue_expected (row_id bigint, day datev2, event_time datetimev2(0))
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_date_docvalue_expected values
        (1,'2024-01-01','2024-01-01 23:00:00'),(2,'2024-01-02','2024-01-02 00:00:00'),(3,null,null)"""
    String expected = "internal.${context.dbName}.es_date_docvalue_expected"
    def originalZone = sql("select @@time_zone")[0][0]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    try {
        sql "set time_zone = 'UTC'"
        [6, 8].each { version ->
            String portKey = "es_${version}_port"
            String port = context.config.otherConfigs.get(portKey)
            assertTrue(port != null && !port.isEmpty(), "es_${version}_port must be configured")
            String endpoint = "http://${host}:${port}"
            def request = { String method, String path, Object body = null ->
                def connection = new URL("${endpoint}/${path}").openConnection()
                connection.requestMethod = method
                connection.connectTimeout = 10000
                connection.readTimeout = 30000
                try {
                    if (body != null) {
                        connection.doOutput = true
                        connection.setRequestProperty("Content-Type", "application/json")
                        connection.outputStream.withCloseable {
                            it.write(JsonOutput.toJson(body).getBytes("UTF-8"))
                        }
                    }
                    int status = connection.responseCode
                    if (method == "DELETE" && status == 404) {
                        return null
                    }
                    String text = (status < 400 ? connection.inputStream : connection.errorStream).getText("UTF-8")
                    assertTrue(status >= 200 && status < 300, "ES ${method} ${path}: ${status} ${text}")
                    return new JsonSlurper().parseText(text)
                } finally {
                    connection.disconnect()
                }
            }
            request("DELETE", "doris_date_docvalue_formats")
            def properties = [row_id: [type: "long"], day: [type: "date", format: "yyyy-MM-dd"],
                event_time: [type: "date", format: "yyyy-MM-dd HH:mm:ss"]]
            // ES 6 bare docvalue_fields return ISO text, while ES 8 uses these mapped formats.
            def mappings = version == 6 ? [_doc: [properties: properties]] : [properties: properties]
            request("PUT", "doris_date_docvalue_formats", [
                settings: [number_of_shards: 1, number_of_replicas: 0], mappings: mappings])
            [[row_id: 1, day: "2024-01-01", event_time: "2024-01-01 23:00:00"],
             [row_id: 2, day: "2024-01-02", event_time: "2024-01-02 00:00:00"],
             [row_id: 3]].each { row ->
                request("PUT", "doris_date_docvalue_formats/_doc/${row.row_id}", row)
            }
            request("POST", "doris_date_docvalue_formats/_refresh")
            [false, true].each { docvalues ->
                String catalog = "es_date_docvalue_${version}_${docvalues}"
                sql "switch internal"
                sql "drop catalog if exists ${catalog}"
                sql """create catalog ${catalog} properties("type"="es", "hosts"="${endpoint}",
                    "nodes_discovery"="false", "enable_docvalue_scan"="${docvalues}")"""
                sql "switch ${catalog}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    quickTest("es_${version}_docvalues_${docvalues}_parallel_${parallel}",
                        "select row_id,day,event_time from doris_date_docvalue_formats order by row_id",
                        "select row_id,day,event_time from ${expected} order by row_id")
                }
            }
        }
    } finally {
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set time_zone = '${originalZone}'"
        sql "switch internal"
    }
}
