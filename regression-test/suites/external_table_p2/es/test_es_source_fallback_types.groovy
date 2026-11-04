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

suite("test_es_source_fallback_types", "p2,external") {
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

    def settings = [number_of_shards: 1, number_of_replicas: 0,
        analysis: [normalizer: [lowercase_value: [type: "custom", filter: ["lowercase"]]]]]
    def index = "doris_source_fallback_types"
    def properties = [row_id: [type: "long"], a: [type: "keyword", normalizer: "lowercase_value"],
        n: [type: "long"], alias_value: [type: "alias", path: "n"],
        generated_value: [type: "long", script: [source: "emit(5)"]],
        items: [type: "long"], event_time: [type: "date"], truth: [type: "boolean"], flags: [type: "boolean"]]
    request("DELETE", index)
    request("PUT", index, [settings: settings, mappings: [
        _meta: [doris: [array_fields: ["items", "flags"]]], properties: properties]])
    [[row_id: 1, a: "ok", n: 5, items: [], event_time: "2024-01-01", truth: "", flags: ["", true, false]],
     [row_id: 2, a: "other", n: 7, items: [1,2], event_time: "2024-01-01T08:00:00+08:00",
        truth: false, flags: [true]],
     [row_id: 3, a: "nullish", n: null, items: null, event_time: null, truth: "false", flags: []],
     [row_id: 4, a: "absent", n: 0, truth: true, flags: null]].each { row ->
        request("PUT", "${index}/_doc/${row.row_id}", row)
    }
    request("POST", "${index}/_refresh")
    ["flag", "mode"].each { kind ->
        def disabled = "doris_source_disabled_${kind}"
        request("DELETE", disabled)
        request("PUT", disabled, [settings: settings, mappings: [
            _source: kind == "flag" ? [enabled: false] : [mode: "disabled"],
            properties: [row_id: properties.row_id, a: properties.a, n: properties.n]]])
        request("PUT", "${disabled}/_doc/1?refresh=true", [row_id: 1, a: "ok", n: 1])
        request("POST", "_aliases", [actions: [[add: [index: index, alias: "doris_source_disabled_alias_${kind}"]],
            [add: [index: disabled, alias: "doris_source_disabled_alias_${kind}"]]]])
    }
    request("DELETE", "doris_source_root_disabled")
    request("PUT", "doris_source_root_disabled", [settings: settings, mappings: [enabled: false,
        properties: [row_id: properties.row_id, a: properties.a, n: properties.n]]])
    request("PUT", "doris_source_root_disabled/_doc/1?refresh=true", [row_id: 1, a: "ok", n: 1])
    request("POST", "_aliases", [actions: [[add: [index: index, alias: "doris_source_root_alias"]],
        [add: [index: "doris_source_root_disabled", alias: "doris_source_root_alias"]]]])
    request("DELETE", "doris_source_integer_coercion")
    request("PUT", "doris_source_integer_coercion", [settings: settings, mappings: [
        properties: [row_id: properties.row_id, a: properties.a, n: properties.n]]])
    [[row_id: 1, a: "exponent", n: "1.2e3"], [row_id: 2, a: "singleton", n: [5]],
     [row_id: 3, a: "negative", n: "-1.9E1"], [row_id: 4, a: "maximum", n: "9223372036854775807"],
     [row_id: 5, a: "fraction", n: "0.9"]].each { row ->
        request("PUT", "doris_source_integer_coercion/_doc/${row.row_id}", row)
    }
    request("POST", "doris_source_integer_coercion/_refresh")
    request("DELETE", "doris_retained_not")
    request("PUT", "doris_retained_not", [settings: settings, mappings: [
        properties: [row_id: properties.row_id, n: properties.n, b: [type: "keyword"]]]])
    [[row_id: 1, n: null, b: "miss"], [row_id: 2, n: 1, b: "miss"],
     [row_id: 3, n: 2, b: "hit"], [row_id: 4, n: 2, b: "miss"]].each { row ->
        request("PUT", "doris_retained_not/_doc/${row.row_id}", row)
    }
    request("POST", "doris_retained_not/_refresh")
    sql "drop table if exists es_source_fallback_expected"
    sql """create table es_source_fallback_expected (
        row_id bigint, a string, n bigint, alias_value bigint, generated_value bigint,
        items array<bigint>, event_time datetimev2(0), truth boolean, flags array<boolean>
    ) distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_source_fallback_expected values
        (1,'ok',5,null,null,[],'2024-01-01 00:00:00',false,[false,true,false]),
        (2,'other',7,null,null,[1,2],'2024-01-01 08:00:00',false,[true]),
        (3,'nullish',null,null,null,null,null,false,[]),
        (4,'absent',0,null,null,null,null,true,null)"""
    def expected = "internal.${context.dbName}.es_source_fallback_expected"
    def queries = [
        ["row_id,a,alias_value", "alias_value = 5"], ["row_id,a,alias_value", "alias_value is null"],
        ["row_id,a,generated_value", "generated_value = 5"], ["row_id,a,generated_value", "generated_value is null"],
        ["row_id,a,items", "items is null"], ["row_id,a,items", "items is not null"],
        ["row_id,a,event_time", "event_time = cast('2024-01-01 08:00:00' as datetimev2(0))"],
        ["row_id,a,event_time", "event_time = cast('2024-01-01 00:00:00' as datetimev2(0))"],
        ["row_id,a,truth,flags", "row_id > 0"], ["row_id,a,truth", "truth = false"]]
    sql "drop table if exists es_integer_coercion_expected"
    sql """create table es_integer_coercion_expected (row_id bigint,a string,n bigint)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_integer_coercion_expected values (1,'exponent',1200),(2,'singleton',5),
        (3,'negative',-19),(4,'maximum',9223372036854775807),(5,'fraction',0)"""
    def integerExpected = "internal.${context.dbName}.es_integer_coercion_expected"
    sql "drop table if exists es_retained_not_expected"
    sql """create table es_retained_not_expected (row_id bigint,n bigint,b string)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql "insert into es_retained_not_expected values (1,null,'miss'),(2,1,'miss'),(3,2,'hit'),(4,2,'miss')"
    def notExpected = "internal.${context.dbName}.es_retained_not_expected"
    def originalNotRules = sql("show variables like 'disable_nereids_expression_rules'")[0][1]
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalZone = sql("show variables like 'time_zone'")[0][1]
    try {
        sql "set time_zone = 'Asia/Shanghai'"
        sql "set disable_nereids_expression_rules = 'SIMPLIFY_NOT_EXPR'"
        [true, false].each { docvalues ->
            def catalog = "es_source_fallback_${docvalues}"
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
                    queries.eachWithIndex { pair, i ->
                        def query = "select ${pair[0]} from ${index} where ${pair[1]} order by row_id"
                        quickTest("${prefix}_source_${i}", query,
                                "select ${pair[0]} from ${expected} where ${pair[1]} order by row_id")
                        quickTest("${prefix}_source_${i}", query)
                    }
                    ["alias_value", "generated_value"].each { column ->
                        def query = "select row_id,${column} from ${index} where ${column} = 5 order by row_id"
                        def predicate = column == "alias_value" ? "n = 5" : "row_id > 0"
                        def oracle = "select row_id,cast(5 as bigint) from ${expected} where " +
                                (docvalues ? predicate : "false") + " order by row_id"
                        quickTest("${prefix}_${column}_control", query, oracle)
                        quickTest("${prefix}_${column}_control", query)
                    }
                    // Check date read-mode differences without depending on the FE JVM timezone
                    // used by the existing docvalue date-literal pushdown conversion.
                    def dateControl = "select row_id,event_time from ${index} where event_time is not null order by row_id"
                    def dateProjection = docvalues ? "cast('2024-01-01 08:00:00' as datetimev2(0))" : "event_time"
                    def dateOracle = "select row_id,${dateProjection} from ${expected} " +
                            "where event_time is not null order by row_id"
                    quickTest("${prefix}_date_control", dateControl, dateOracle)
                    quickTest("${prefix}_date_control", dateControl)
                    def boolControl = "select truth from ${index} where row_id = 1"
                    quickTest("${prefix}_boolean_control", boolControl, "select cast(false as boolean)")
                    quickTest("${prefix}_boolean_control", boolControl)
                    ["true", "n = 1200", "n = 5", "n < 0"].eachWithIndex { predicate, i ->
                        def query = "select row_id,a,n from doris_source_integer_coercion where ${predicate} order by row_id"
                        quickTest("${prefix}_integer_${i}", query,
                                "select row_id,a,n from ${integerExpected} where ${predicate} order by row_id")
                        quickTest("${prefix}_integer_${i}", query)
                        def control = "select row_id,n from doris_source_integer_coercion where ${predicate} order by row_id"
                        quickTest("${prefix}_integer_control_${i}", control,
                                "select row_id,n from ${integerExpected} where ${predicate} order by row_id")
                        quickTest("${prefix}_integer_control_${i}", control)
                    }
                    ["not(n <> 1)", "not(n = 1)", "not(not(n <> 1))", "not(n < 1 or n > 2)"].eachWithIndex { predicate, i ->
                        def query = "select row_id,n from doris_retained_not where ${predicate} " +
                                "or esquery(b, '{\"term\":{\"b\":\"hit\"}}') order by row_id"
                        quickTest("${prefix}_retained_not_${i}", query,
                                "select row_id,n from ${notExpected} where ${predicate} or b = 'hit' order by row_id")
                        quickTest("${prefix}_retained_not_${i}", query)
                    }
                    ["doris_source_root_disabled", "doris_source_root_alias"].eachWithIndex { target, i ->
                        ["n is null", "n = 1"].eachWithIndex { predicate, j ->
                            def query = "select a,n from ${target} where ${predicate} order by a"
                            def oracle = "select a,n from (select 'ok' as a,cast(1 as bigint) as n) r where ${predicate}"
                            if (i == 1) {
                                oracle = "select a,n from (${oracle} union all select a,n from ${expected} " +
                                        "where ${predicate}) t order by a"
                            }
                            quickTest("${prefix}_root_${i}_${j}", query, oracle)
                            quickTest("${prefix}_root_${i}_${j}", query)
                        }
                    }
                    ["flag", "mode"].each { kind ->
                        ["doris_source_disabled_${kind}", "doris_source_disabled_alias_${kind}"].eachWithIndex { target, i ->
                            def query = "select a,n from ${target} where n = 1"
                            quickTest("${prefix}_disabled_${kind}_${i}", query,
                                    "select cast(null as string),cast(null as bigint) where false")
                            quickTest("${prefix}_disabled_${kind}_${i}", query)
                            def allRows = "select a,n from ${target} order by a"
                            def oracle = "select cast(null as string) as a,cast(null as bigint) as n"
                            if (i == 1) {
                                oracle = "select a,n from (${oracle} union all select a,n from ${expected}) t order by a"
                            }
                            quickTest("${prefix}_disabled_rows_${kind}_${i}", allRows, oracle)
                            quickTest("${prefix}_disabled_rows_${kind}_${i}", allRows)
                        }
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set time_zone = '${originalZone}'"
        sql "set disable_nereids_expression_rules = '${originalNotRules}'"
    }
}
