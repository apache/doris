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

suite("test_es_epoch_millis", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableEsTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("es_8_port")
    assertTrue(port != null && !port.isEmpty(), "es_8_port must be configured")
    def endpoint = "http://${host}:${port}"
    def request = { String method, String path, Object body = null ->
        def connection = new URL("${endpoint}/${path}").openConnection()
        connection.requestMethod = method
        connection.connectTimeout = 10000
        connection.readTimeout = 30000
        try {
            if (body != null) {
                connection.doOutput = true
                connection.setRequestProperty("Content-Type", "application/json")
                connection.outputStream.withCloseable { it.write(JsonOutput.toJson(body).getBytes("UTF-8")) }
            }
            int status = connection.responseCode
            if (method == "DELETE" && status == 404) {
                return null
            }
            def text = (status < 400 ? connection.inputStream : connection.errorStream).getText("UTF-8")
            assertTrue(status >= 200 && status < 300, "ES ${method} ${path}: ${status} ${text}")
            return new JsonSlurper().parseText(text)
        } finally {
            connection.disconnect()
        }
    }
    request("DELETE", "doris_epoch_millis")
    request("PUT", "doris_epoch_millis", [settings: [number_of_shards: 1, number_of_replicas: 0,
        analysis: [normalizer: [folded: [type: "custom", filter: ["lowercase"]]]]],
        mappings: [properties: [row_id: [type: "long"],
            a: [type: "keyword", normalizer: "folded"], d: [type: "date"]]]])
    // Projecting normalized a forces source decoding of every d, including numeric JSON doubles.
    ["0", "0000000000000", "1.5", new BigDecimal("1.5"), "1000.", "12345",
        "-0.000001", new BigDecimal("-1.5"), "-1000.5", new BigDecimal("1677895728000.5"),
        null, ["0"], [], [null]].eachWithIndex { value, i ->
        request("PUT", "doris_epoch_millis/_doc/${i + 1}", [row_id: i + 1, a: "VALUE", d: value])
    }
    request("POST", "doris_epoch_millis/_refresh")

    sql "drop table if exists es_epoch_millis_expected"
    sql """create table es_epoch_millis_expected (row_id bigint,a string,d datetimev2(0))
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_epoch_millis_expected values
        (1,'VALUE','1970-01-01 00:00:00'),(2,'VALUE','1970-01-01 00:00:00'),
        (3,'VALUE','1970-01-01 00:00:00'),(4,'VALUE','1970-01-01 00:00:00'),
        (5,'VALUE','1970-01-01 00:00:01'),(6,'VALUE','1970-01-01 00:00:12'),
        (7,'VALUE','1969-12-31 23:59:59'),(8,'VALUE','1969-12-31 23:59:59'),
        (9,'VALUE','1969-12-31 23:59:58'),(10,'VALUE','2023-03-04 02:08:48'),
        (11,'VALUE',null),(12,'VALUE','1970-01-01 00:00:00'),(13,'VALUE',null),(14,'VALUE',null)"""
    def expected = "internal.${context.dbName}.es_epoch_millis_expected"
    // SHOW prefixes deprecated variable names; scalar access keeps the compatibility name stable.
    def originalV2 = sql("SELECT @@enable_file_scanner_v2")[0][0]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalZone = sql("show variables like 'time_zone'")[0][1]
    try {
        [true, false].each { docvalues ->
            def catalog = "es_epoch_millis_${docvalues}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties("type"="es","hosts"="${endpoint}",
                "nodes_discovery"="false","enable_docvalue_scan"="${docvalues}")"""
            sql "switch ${catalog}"
            [false, true].each { scanner ->
                sql "set enable_file_scanner_v2 = ${scanner}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    [0, 8].each { hours ->
                        sql "set time_zone = '+0${hours}:00'"
                        def prefix = "dv_${docvalues}_v2_${scanner}_parallel_${parallel}_zone_${hours}"
                        def oracle = "(select row_id,a,date_add(d,interval ${hours} hour) as d from ${expected}) e"
                        ["true", "d is null", "d < '1970-01-01 00:00:00'",
                            "d = '1970-01-01 08:00:00'"].eachWithIndex { predicate, i ->
                            def query = "select row_id,a,d from doris_epoch_millis where ${predicate} order by row_id"
                            quickTest("${prefix}_source_${i}", query,
                                    "select row_id,a,d from ${oracle} where ${predicate} order by row_id")
                            quickTest("${prefix}_source_${i}", query)
                        }
                        def control = "select row_id,d from doris_epoch_millis order by row_id"
                        quickTest("${prefix}_control", control,
                                "select row_id,d from ${oracle} order by row_id")
                        quickTest("${prefix}_control", control)
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set time_zone = '${originalZone}'"
    }
}
