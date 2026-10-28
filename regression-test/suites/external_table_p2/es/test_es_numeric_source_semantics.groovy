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

suite("test_es_numeric_source_semantics", "p2,external") {
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

    def index = "doris_numeric_source_safety"
    request("DELETE", index)
    request("PUT", index, [
        settings: [number_of_shards: 1, number_of_replicas: 0,
            analysis: [normalizer: [lowercase_value: [type: "custom", filter: ["lowercase"]]]]],
        mappings: [properties: [row_id: [type: "long"],
            a: [type: "keyword", normalizer: "lowercase_value"],
            b: [type: "scaled_float", scaling_factor: 4], c: [type: "half_float"]]]])
    [[row_id: 1, a: "ok", b: 2.34, c: 2.34], [row_id: 2, a: "empty", b: null, c: null],
     [row_id: 3, a: "other", b: -2.34, c: -2.34]].each { row ->
        request("PUT", "${index}/_doc/${row.row_id}", row)
    }
    request("POST", "${index}/_refresh")

    sql "drop table if exists es_numeric_source_expected"
    sql """create table es_numeric_source_expected (row_id bigint, a string, b double, c float)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_numeric_source_expected values
        (1,'ok',2.34,2.34),(2,'empty',null,null),(3,'other',-2.34,-2.34)"""
    def expectedTable = "internal.${context.dbName}.es_numeric_source_expected"
    def predicates = ["b = 2.25", "b = 2.34", "b > 2.25", "b < 2.30", "b != 2.25",
        "b in (2.34, -2.34)", "b not in (2.25)", "not (b = 2.25)", "b is null", "b is not null",
        "b = 2.25 or row_id = 3", "not (b > 2.25 and row_id = 1)",
        "c = cast(2.34 as float)", "c = cast(2.33984375 as float)",
        "c > cast(2.33984375 as float)", "c in (cast(2.34 as float), cast(-2.34 as float))"]
    def nativeTerm = """esquery(b, '{"term":{"b":2.25}}')"""
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    try {
        ["enabled", "disabled", "limited"].each { mode ->
            def catalog = "es_numeric_source_${mode}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties (
                "type"="es", "hosts"="${endpoint}", "nodes_discovery"="false",
                "enable_docvalue_scan"="${mode != 'disabled'}",
                "max_docvalue_fields"="${mode == 'limited' ? 1 : 20}"
            )"""
            sql "switch ${catalog}"
            [false, true].each { scannerV2 ->
                sql "set enable_file_scanner_v2 = ${scannerV2}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    def tag = "${mode}_v2_${scannerV2}_parallel_${parallel}"
                    // The normalizer forces the whole projection to source. Numeric predicates must
                    // compare the original values, even when the indexed representation is rounded.
                    predicates.eachWithIndex { predicate, i ->
                        def query = "select row_id, a, b, c from ${index} where ${predicate} order by row_id"
                        quickTest("${tag}_predicate_${i}", query,
                                "select row_id, a, b, c from ${expectedTable} where ${predicate} order by row_id")
                        quickTest("${tag}_predicate_${i}", query)
                    }
                    def filterOnly = "select a from ${index} where b = 2.25 order by a"
                    quickTest("${tag}_filter_only", filterOnly,
                            "select a from ${expectedTable} where b = 2.25 order by a")
                    quickTest("${tag}_filter_only", filterOnly)
                    def nativeQuery = "select a, b from ${index} where ${nativeTerm}"
                    quickTest("${tag}_native", nativeQuery, "select a, b from ${expectedTable} where row_id = 1")
                    quickTest("${tag}_native", nativeQuery)
                    def nativeAnd = "select a, b from ${index} where b = 2.25 and ${nativeTerm}"
                    quickTest("${tag}_native_and", nativeAnd,
                            "select a, b from ${expectedTable} where false")
                    quickTest("${tag}_native_and", nativeAnd)
                    test {
                        sql "select a, b from ${index} where b = 2.25 or ${nativeTerm}"
                        exception "Cannot evaluate esquery locally"
                    }
                    // A numeric-only projection retains docvalues and its existing indexed-value
                    // semantics. This control prevents fixing source predicates by disabling docvalues.
                    ["b": "2.25", "c": "2.33984375"].each { column, rounded ->
                        def query = "select ${column} from ${index} where ${column} = ${rounded}"
                        def oracle = "select cast(${rounded} as ${column == 'b' ? 'double' : 'float'})" +
                                (mode == "disabled" ? " where false" : "")
                        quickTest("${tag}_${column}_docvalue_control", query, oracle)
                        quickTest("${tag}_${column}_docvalue_control", query)
                    }
                    // With no normalized column projected, only the field limit or catalog flag
                    // forces source mode; the same numeric guard must apply to both fallbacks.
                    def numericOnly = "select row_id, b, c from ${index} where b = 2.25"
                    def numericOracle = "select cast(1 as bigint), cast(2.25 as double), " +
                            "cast(2.33984375 as float)" + (mode == "enabled" ? "" : " where false")
                    quickTest("${tag}_numeric_projection", numericOnly, numericOracle)
                    quickTest("${tag}_numeric_projection", numericOnly)
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
    }
}
