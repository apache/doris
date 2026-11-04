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

suite("test_es_mapping_boundaries", "p2,external") {
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

    def settings = [number_of_shards: 1, number_of_replicas: 0]
    def populated = "doris_mapping_boundary_m"
    request("DELETE", populated)
    request("PUT", populated, [settings: settings, mappings: [properties: [
        row_id: [type: "long"], amount: [type: "long", index: false, doc_values: true],
        event_time: [type: "date", format: "epoch_millis", index: false],
        flag: [type: "boolean", index: false], tag: [type: "keyword"]]]])
    [[row_id: 1, amount: 1, event_time: 1700000000000, flag: true, tag: "one"],
     [row_id: 2, amount: 2, event_time: 1700000001000, flag: false, tag: "ok"],
     [row_id: 3, amount: 3, event_time: 1700000002000, flag: true, tag: "other"],
     [row_id: 4, amount: null, event_time: null, flag: false, tag: "none"]].each { row ->
        request("PUT", "${populated}/_doc/${row.row_id}", row)
    }
    request("POST", "${populated}/_refresh")
    def aliases = []
    [[enabled: false], [date_detection: false], [numeric_detection: true],
     [dynamic_date_formats: ["yyyy-MM-dd"]], [_source: [enabled: false]], [dynamic: false]].eachWithIndex { config, i ->
        ["a", "z"].each { order ->
            def backing = "doris_mapping_boundary_${order}_${i}"
            def alias = "doris_mapping_boundary_alias_${order}_${i}"
            request("DELETE", backing)
            request("PUT", backing, [settings: settings, mappings: config])
            def sourceOnly = config.enabled == false || config.dynamic == false
            if (sourceOnly) {
                // Disabled parsing still stores source documents, which alias scans must retain.
                request("PUT", "${backing}/_doc/99?refresh=true", [row_id: 99, amount: 99])
            }
            request("POST", "_aliases", [actions: [
                [add: [index: populated, alias: alias]], [add: [index: backing, alias: alias]]]])
            aliases.add([name: alias, sourceOnly: sourceOnly])
        }
    }
    sql "drop table if exists es_mapping_boundary_expected"
    sql """create table es_mapping_boundary_expected (
        row_id bigint, amount bigint, event_time bigint, flag boolean, tag string
    ) distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_mapping_boundary_expected values
        (1,1,1700000000000,true,'one'),(2,2,1700000001000,false,'ok'),
        (3,3,1700000002000,true,'other'),(4,null,null,false,'none')"""
    def expected = "internal.${context.dbName}.es_mapping_boundary_expected"
    def nativeTerm = """esquery(tag, '{"term":{"tag":"ok"}}')"""
    def predicates = ["amount = 1 or ${nativeTerm}", "not (amount = 1 or ${nativeTerm})",
        "amount = 1 and ${nativeTerm}", "amount in (1,3) or ${nativeTerm}",
        "amount > 1 or ${nativeTerm}", "flag = true or ${nativeTerm}",
        "not (flag = true or ${nativeTerm})", "event_time = 1700000000000 or ${nativeTerm}",
        "not (event_time = 1700000000000 or ${nativeTerm})", "event_time > 1700000000000 or ${nativeTerm}"]
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    try {
        [true, false].each { docvalues ->
            def catalog = "es_mapping_boundary_${docvalues}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties (
                "type"="es", "hosts"="${endpoint}", "nodes_discovery"="false",
                "enable_docvalue_scan"="${docvalues}"
            )"""
            sql "switch ${catalog}"
            [false, true].each { scannerV2 ->
                sql "set enable_file_scanner_v2 = ${scannerV2}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    def prefix = "dv_${docvalues}_v2_${scannerV2}_parallel_${parallel}"
                    predicates.eachWithIndex { predicate, i ->
                        def query = "select row_id, amount, event_time, flag from ${populated} " +
                                "where ${predicate} order by row_id"
                        quickTest("${prefix}_native_${i}", query,
                                "select row_id, amount, event_time, flag from ${expected} " +
                                "where ${predicate.replace(nativeTerm, "tag = 'ok'")} order by row_id")
                        quickTest("${prefix}_native_${i}", query)
                    }
                    aliases.eachWithIndex { alias, i ->
                        def query = "select row_id, amount from ${alias.name} order by row_id"
                        def oracle = "select row_id, amount from ${expected}" +
                                (alias.sourceOnly ? " union all select cast(99 as bigint), cast(99 as bigint)" : "")
                        quickTest("${prefix}_alias_${i}", query, "select * from (${oracle}) t order by row_id")
                        quickTest("${prefix}_alias_${i}", query)
                        // Fields missing from a configuration-only backing cannot narrow source hits.
                        def filterQuery = "select row_id from ${alias.name} where amount = 99 order by row_id"
                        quickTest("${prefix}_alias_filter_${i}", filterQuery,
                                "select cast(99 as bigint)" + (alias.sourceOnly ? "" : " where false"))
                        quickTest("${prefix}_alias_filter_${i}", filterQuery)
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
    }
}
