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

suite("test_es_scalar_materialization", "p2,external") {
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
        analysis: [normalizer: [folded: [type: "custom", filter: ["lowercase"]]]]]
    request("DELETE", "doris_scalar_materialization")
    request("PUT", "doris_scalar_materialization", [settings: settings, mappings: [
        _meta: [doris: [array_fields: ["narr"]]], properties: [row_id: [type: "long"],
        a: [type: "keyword", normalizer: "folded"], f: [type: "float"], d: [type: "double"],
        truth: [type: "boolean"], day: [type: "date"], n: [type: "byte"], s: [type: "short"],
        i: [type: "integer"], l: [type: "long"], narr: [type: "byte"]]]])
    [[row_id: 1, a: "one", f: [1.5], d: [2.5], truth: [""], day: ["2024-01-01"],
        n: "0.99999999999999999", s: "0.99999999999999999", i: "0.99999999999999999",
        l: "0.99999999999999999", narr: ["0.99999999999999999", "-0.99999999999999999"]],
     [row_id: 2, a: "two", f: [], d: [], truth: [], day: [], n: "-0.99999999999999999",
        s: "-0.99999999999999999", i: "-0.99999999999999999", l: "-0.99999999999999999", narr: []],
     [row_id: 3, a: "nullish", f: [null], d: [null], truth: [null], day: [null]],
     [row_id: 4, a: "missing"],
     [row_id: 5, a: "scalar", f: 3.5, d: 4.5, truth: true, day: "2024-01-01",
        n: "127.9", s: 32767.9, i: 2147483647, l: "9223372036854775807", narr: [127]],
     [row_id: 6, a: "negative", f: "1.25", d: "2.25", truth: "true", day: "2024-01-01T08:00:00+08:00",
        n: "-128.9", s: "-32768", i: "-2147483648", l: "-9223372036854775808", narr: [-128]],
     [row_id: 7, a: "nan", n: "NaN", s: "NaN", i: "NaN", narr: ["NaN"]]].each { row ->
        request("PUT", "doris_scalar_materialization/_doc/${row.row_id}", row)
    }
    request("POST", "doris_scalar_materialization/_refresh")
    request("DELETE", "doris_retained_range_not")
    request("PUT", "doris_retained_range_not", [settings: settings, mappings: [properties: [
        row_id: [type: "long"], n: [type: "long"], b: [type: "keyword"]]]])
    [[row_id: 1, n: [1,2], b: "miss"], [row_id: 2, n: [2,3], b: "miss"],
     [row_id: 3, n: [3], b: "miss"], [row_id: 4, n: null, b: "miss"],
     [row_id: 5, n: [1,2], b: "hit"]].each { row ->
        request("PUT", "doris_retained_range_not/_doc/${row.row_id}", row)
    }
    request("POST", "doris_retained_range_not/_refresh")
    sql "drop table if exists es_scalar_materialization_expected"
    sql """create table es_scalar_materialization_expected (
        row_id bigint,a string,f float,d double,truth boolean,day datetimev2(0),n tinyint,s smallint,
        i int,l bigint,narr array<tinyint>
    ) distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_scalar_materialization_expected values
        (1,'one',1.5,2.5,false,'2024-01-01 00:00:00',1,1,1,0,[1,-1]),
        (2,'two',null,null,null,null,-1,-1,-1,0,[]),
        (3,'nullish',null,null,null,null,null,null,null,null,null),
        (4,'missing',null,null,null,null,null,null,null,null,null),
        (5,'scalar',3.5,4.5,true,'2024-01-01 00:00:00',127,32767,2147483647,9223372036854775807,[127]),
        (6,'negative',1.25,2.25,true,'2024-01-01 08:00:00',-128,-32768,-2147483648,-9223372036854775808,[-128]),
        (7,'nan',null,null,null,null,0,0,0,null,[0])"""
    def expected = "internal.${context.dbName}.es_scalar_materialization_expected"
    sql "drop table if exists es_retained_range_expected"
    sql """create table es_retained_range_expected (row_id bigint,n bigint,b string)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql "insert into es_retained_range_expected values (1,1,'miss'),(2,2,'miss'),(3,3,'miss'),(4,null,'miss'),(5,1,'hit')"
    def rangeExpected = "internal.${context.dbName}.es_retained_range_expected"
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalZone = sql("show variables like 'time_zone'")[0][1]
    def originalRules = sql("show variables like 'disable_nereids_expression_rules'")[0][1]
    try {
        sql "set time_zone = 'Asia/Shanghai'"
        sql "set disable_nereids_expression_rules = 'SIMPLIFY_NOT_EXPR'"
        [true, false].each { docvalues ->
            def catalog = "es_scalar_materialization_${docvalues}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties("type"="es","hosts"="${endpoint}",
                "nodes_discovery"="false","enable_docvalue_scan"="${docvalues}")"""
            sql "switch ${catalog}"
            [false, true].each { scanner ->
                sql "set enable_file_scanner_v2 = ${scanner}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    def prefix = "dv_${docvalues}_v2_${scanner}_parallel_${parallel}"
                    def predicates = ["true", "f is null", "d is not null", "truth is null", "day is not null",
                        "n = 1", "s = 1", "i = 1", "n < 0", "s < 0", "i < 0", "l = 0"]
                    predicates.eachWithIndex { predicate, j ->
                        def projection = "row_id,a,f,d,truth,day,n,s,i,l,narr"
                        def query = "select ${projection} from doris_scalar_materialization where ${predicate} order by row_id"
                        quickTest("${prefix}_source_${j}", query,
                                "select ${projection} from ${expected} where ${predicate} order by row_id")
                        quickTest("${prefix}_source_${j}", query)
                    }
                    ["f", "d", "truth", "n", "s", "i", "l"].each { column ->
                        def query = "select row_id,${column} from doris_scalar_materialization order by row_id"
                        quickTest("${prefix}_${column}_control", query,
                                "select row_id,${column} from ${expected} order by row_id")
                        quickTest("${prefix}_${column}_control", query)
                    }
                    if (docvalues) {
                        // Numeric docvalues select the first sorted value. These LT/LE complements
                        // exercise arrays spanning the threshold, scalar controls and missing values.
                        ["n < 2", "n <= 2"].eachWithIndex { predicate, j ->
                            def query = "select row_id,n from doris_retained_range_not where not(${predicate}) " +
                                    "or esquery(b, '{\"term\":{\"b\":\"hit\"}}') order by row_id"
                            quickTest("${prefix}_range_not_${j}", query,
                                    "select row_id,n from ${rangeExpected} where not(${predicate}) or b = 'hit' order by row_id")
                            quickTest("${prefix}_range_not_${j}", query)
                        }
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set time_zone = '${originalZone}'"
        sql "set disable_nereids_expression_rules = '${originalRules}'"
    }
}
