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

suite("test_es_source_value_boundaries", "p2,external") {
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
    request("DELETE", "doris_source_value_boundaries")
    request("PUT", "doris_source_value_boundaries", [settings: settings, mappings: [
        _meta: [doris: [array_fields: ["numbers", "truths", "days"]]], properties: [
        row_id: [type: "long"], a: [type: "keyword", normalizer: "folded"], obj: [type: "object"],
        n: [type: "long"], f: [type: "float"], d: [type: "double"], i: [type: "integer"], s: [type: "short"],
        numbers: [type: "long"], truths: [type: "boolean"], days: [type: "date"]]]])
    // BigDecimal keeps an unquoted floating JSON token rather than an exact integer/string token.
    [[row_id: 1, a: "ONE", obj: [[x: 1]], n: new BigDecimal("-9223372036854775808.0"),
        f: "", d: "", i: "0x1.8p1", s: "3d", numbers: [null, "", 1, null],
        truths: [null, true, false, null], days: [null, "2024-01-01", null]],
     [row_id: 2, a: "TWO", obj: [], n: "", f: "0x1.8p1f", d: "3d", i: "+3F", s: " 3d ",
        numbers: [null], truths: [null], days: []],
     [row_id: 3, a: "THREE", obj: [null], n: null,
        f: "1.000000059604644775390625000000000000000000000001", d: 1.5, i: "-0x1.8p1", s: "-3d",
        numbers: [], truths: [], days: [null]],
     [row_id: 4, a: "FOUR", obj: [x: 1], n: new BigDecimal("9223372036854775808.0"),
        f: 2.5, d: 2.5, i: 3, s: 3]].each { row ->
        request("PUT", "doris_source_value_boundaries/_doc/${row.row_id}", row)
    }
    request("POST", "doris_source_value_boundaries/_refresh")

    ["doris_inherited_mapping", "doris_safe_mapping"].eachWithIndex { index, j ->
        request("DELETE", index)
        def indexSettings = new LinkedHashMap(settings)
        indexSettings["index.mapping.ignore_malformed"] = (j == 0)
        indexSettings["index.mapping.coerce"] = (j != 0)
        request("PUT", index, [settings: indexSettings, mappings: [properties: [
            row_id: [type: "long"], a: [type: "keyword", normalizer: "folded"], b: [type: "long"],
            c: [type: "long", ignore_malformed: false]]]])
        request("PUT", "${index}/_doc/1", [row_id: j + 1, a: "VALUE", b: "5", c: 5])
        request("POST", "${index}/_refresh")
    }
    request("POST", "_aliases", [actions: [
        [add: [index: "doris_inherited_mapping", alias: "doris_mapping_boundaries_alias"]],
        [add: [index: "doris_safe_mapping", alias: "doris_mapping_boundaries_alias"]]]])

    sql "drop table if exists es_source_value_boundaries_expected"
    sql """create table es_source_value_boundaries_expected (
        row_id bigint,a string,obj json,n bigint,f float,d double,i int,s smallint,
        numbers array<bigint>,truths array<boolean>,days array<datetimev2(0)>
    ) distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_source_value_boundaries_expected values
        (1,'ONE','[{"x":1}]',-9223372036854775808,null,null,3,3,
            [null,null,1,null],[null,true,false,null],[null,'2024-01-01 00:00:00',null]),
        (2,'TWO','[]',null,3,3,3,3,[null],[null],[]),
        (3,'THREE','[null]',null,1.00000011920928955078125,1.5,-3,-3,[],[],[null]),
        (4,'FOUR','{"x":1}',9223372036854775807,2.5,2.5,3,3,null,null,null)"""
    def expected = "internal.${context.dbName}.es_source_value_boundaries_expected"
    sql "drop table if exists es_inherited_mapping_expected"
    sql """create table es_inherited_mapping_expected (row_id bigint,a string,b bigint,c bigint)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql "insert into es_inherited_mapping_expected values (1,'VALUE',5,5),(2,'VALUE',5,5)"
    def mappingExpected = "internal.${context.dbName}.es_inherited_mapping_expected"
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalZone = sql("show variables like 'time_zone'")[0][1]
    try {
        sql "set time_zone = '+00:00'"
        [true, false].each { docvalues ->
            def catalog = "es_source_value_boundaries_${docvalues}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties("type"="es","hosts"="${endpoint}",
                "nodes_discovery"="false","enable_docvalue_scan"="${docvalues}")"""
            sql "switch ${catalog}"
            [false, true].each { scanner ->
                sql "set enable_file_scanner_v2 = ${scanner}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    def prefix = "dv_${docvalues}_v2_${scanner}_parallel_${parallel}"
                    ["true", "n is null", "f is null", "i = 3", "n < 0"].eachWithIndex { predicate, j ->
                        def projection = "row_id,a,cast(obj as string),n,f,d,i,s,numbers,truths,days"
                        def query = "select ${projection} from doris_source_value_boundaries " +
                                "where ${predicate} order by row_id"
                        quickTest("${prefix}_source_${j}", query,
                                "select ${projection} from ${expected} where ${predicate} order by row_id")
                        quickTest("${prefix}_source_${j}", query)
                    }
                    ["cast(obj as string)", "n", "f", "i"].eachWithIndex { projection, j ->
                        def query = "select row_id,${projection} from doris_source_value_boundaries order by row_id"
                        quickTest("${prefix}_control_${j}", query,
                                "select row_id,${projection} from ${expected} order by row_id")
                        quickTest("${prefix}_control_${j}", query)
                    }
                    ["doris_inherited_mapping", "doris_safe_mapping", "doris_mapping_boundaries_alias"].eachWithIndex {
                            index, j ->
                        def rowPredicate = j == 0 ? "row_id=1" : (j == 1 ? "row_id=2" : "true")
                        ["b is null", "b is not null", "b=5", "c=5"].eachWithIndex { predicate, k ->
                            def query = "select row_id,a,b,c from ${index} where ${predicate} order by row_id"
                            quickTest("${prefix}_mapping_${j}_${k}", query,
                                    "select row_id,a,b,c from ${mappingExpected} " +
                                            "where ${rowPredicate} and ${predicate} order by row_id")
                            quickTest("${prefix}_mapping_${j}_${k}", query)
                        }
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
