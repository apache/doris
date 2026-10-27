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

suite("test_es_source_semantics", "p2,external") {
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

    def properties = [
        row_id: [type: "long"], tenant: [type: "keyword"],
        limited: [type: "keyword", ignore_above: 4, doc_values: true],
        normalized: [type: "keyword", normalizer: "lowercase_value"],
        nullable_value: [type: "keyword", null_value: "sentinel"],
        body: [type: "text", fields: [raw: [type: "keyword", ignore_above: 4]]],
        copy_input: [type: "keyword", copy_to: "copied"], copied: [type: "keyword"],
        multi_body: [type: "text", fields: [
            aa_folded: [type: "keyword", normalizer: "lowercase_value"],
            zz_raw: [type: "keyword", doc_values: true]]],
        doconly: [type: "keyword", index: false, doc_values: true]
    ]
    def settings = [number_of_shards: 2, number_of_replicas: 0,
        analysis: [normalizer: [lowercase_value: [type: "custom", filter: ["lowercase"]]]]]
    ["doris_source_safety_a", "doris_source_safety_b", "doris_source_safety_hidden",
     "doris_source_safety_included"].each { index ->
        request("DELETE", index)
        def mappings = [properties: properties]
        if (index.endsWith("hidden")) {
            mappings._source = [excludes: ["limit*"]]
        } else if (index.endsWith("included")) {
            mappings._source = [includes: ["row_*", "tenant", "multi_*", "doconly"]]
        }
        request("PUT", index, [settings: settings, mappings: mappings])
    }

    request("DELETE", "doris_source_safety_z_empty")
    request("PUT", "doris_source_safety_z_empty", [settings: [number_of_shards: 2, number_of_replicas: 0]])

    // Use distinct shards so the routing-alias oracle cannot pass because two routing keys collide.
    def routingKeys = (0..<32).collect { "route_${it}" }
    def firstRouting = routingKeys[0]
    def firstShard = request("GET", "doris_source_safety_a/_search_shards?routing=${firstRouting}").shards[0][0].shard
    def secondRouting = routingKeys.find {
        request("GET", "doris_source_safety_a/_search_shards?routing=${it}").shards[0][0].shard != firstShard
    }
    assertTrue(secondRouting != null, "fixture requires two distinct routing shards")

    def sourceRows = [
        [row_id: 1, tenant: "A", limited: "elephant", normalized: "MiXeD", nullable_value: null,
            body: "elephant", copy_input: "copy-one", copied: null, multi_body: "cat", doconly: "cat"],
        [row_id: 2, tenant: "B", limited: "cat", normalized: "plain", body: "cat", multi_body: "dog", doconly: "dog"],
        [row_id: 3, tenant: "A", limited: "rhino", normalized: "UPPER", nullable_value: "sentinel",
            body: "rhino", copied: "original", multi_body: "Lion", doconly: "cat"],
        [row_id: 4, tenant: "B", limited: null, normalized: null, nullable_value: "other"]
    ]
    ["doris_source_safety_a", "doris_source_safety_b", "doris_source_safety_hidden",
     "doris_source_safety_included"].each { index ->
        sourceRows.eachWithIndex { row, offset ->
            def document = new LinkedHashMap(row)
            if (index.endsWith("_b")) {
                document.row_id += 4
            }
            def routing = offset < 2 ? firstRouting : secondRouting
            request("PUT", "${index}/_doc/${document.row_id}?routing=${routing}", document)
        }
        request("POST", "${index}/_refresh")
    }
    request("POST", "_aliases", [actions: [
        [add: [index: "doris_source_safety_a", alias: "doris_source_with_empty"]],
        [add: [index: "doris_source_safety_z_empty", alias: "doris_source_with_empty"]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_filtered", filter: [term: [tenant: "A"]]]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_multi", filter: [term: [tenant: "A"]]]],
        [add: [index: "doris_source_safety_b", alias: "doris_source_multi", filter: [term: [tenant: "B"]]]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_routed", search_routing: firstRouting]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_filtered_routed",
            search_routing: firstRouting, filter: [term: [tenant: "A"]]]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_empty", filter: [term: [tenant: "absent"]]]],
        [add: [index: "doris_source_safety_a", alias: "doris_source_unfiltered"]],
        [add: [index: "doris_source_safety_b", alias: "doris_source_unfiltered"]]
    ]])

    sql "drop table if exists es_source_safety_expected"
    sql """create table es_source_safety_expected (
        row_id bigint, tenant string, limited string, normalized string, nullable_value string,
        body string, copy_input string, copied string, multi_body string, doconly string
    ) distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    sql """insert into es_source_safety_expected values
        (1,'A','elephant','MiXeD',NULL,'elephant','copy-one',NULL,'cat','cat'),
        (2,'B','cat','plain',NULL,'cat',NULL,NULL,'dog','dog'),
        (3,'A','rhino','UPPER','sentinel','rhino',NULL,'original','Lion','cat'),
        (4,'B',NULL,NULL,'other',NULL,NULL,NULL,NULL,NULL)"""
    def expectedTable = "internal.${context.dbName}.es_source_safety_expected"
    def predicates = [
        "limited = 'elephant'", "limited in ('elephant', 'cat')", "limited != 'cat'",
        "limited is null", "limited is not null", "limited <=> null",
        "limited like 'ele%'", "limited regexp 'ele.*'",
        "body = 'elephant'", "body in ('elephant', 'cat')",
        "normalized = 'MiXeD'", "normalized = 'mixed'", "normalized < 'Z'",
        "nullable_value is null", "nullable_value is not null", "nullable_value = 'sentinel'",
        "copied is null", "copied = 'copy-one'",
        "(limited = 'elephant' and tenant = 'A') or row_id = 2",
        "not (limited = 'elephant' and tenant = 'A')"
    ]
    def nativeTerm = """esquery(tenant, '{"term":{"tenant":"A"}}')"""
    def mixedNativePredicates = [
        ["tenant = 'B' or ${nativeTerm}", "tenant = 'B' or tenant = 'A'"],
        ["multi_body = 'cat' or ${nativeTerm}", "multi_body = 'cat' or tenant = 'A'"],
        ["doconly = 'cat' or ${nativeTerm}", "doconly = 'cat' or tenant = 'A'"],
        ["not (tenant = 'B' or ${nativeTerm})", "not (tenant = 'B' or tenant = 'A')"],
        ["limited = 'elephant' and not (${nativeTerm})", "limited = 'elephant' and not (tenant = 'A')"],
        ["limited = 'elephant' and ${nativeTerm}", "limited = 'elephant' and tenant = 'A'"]
    ]
    request("DELETE", "doris_source_safety_arrays")
    request("PUT", "doris_source_safety_arrays", [settings: [number_of_shards: 2, number_of_replicas: 0],
        mappings: [properties: [row_id: properties.row_id, limited: properties.limited, body: properties.body,
            sibling: [type: "keyword"]]]])
    def arrayValues = [["cat", "elephant"], ["cat"], [], [null], "cat", "elephant"]
    def siblingValues = [["cat"], "cat", ["cat", "dog"], [], [null], null]
    arrayValues.eachWithIndex { value, i ->
        request("PUT", "doris_source_safety_arrays/_doc/${i + 1}",
                [row_id: i + 1, limited: value, body: value, sibling: siblingValues[i]])
    }
    request("POST", "doris_source_safety_arrays/_refresh")
    sql "drop table if exists es_source_array_expected"
    sql """create table es_source_array_expected (row_id bigint, limited string, body string, sibling string)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    def arrayRows = arrayValues.withIndex().collect { value, i ->
        String text = value instanceof List ? JsonOutput.toJson(value) : value
        def sibling = siblingValues[i]
        String siblingSql = sibling == null ? "NULL"
                : "'${sibling instanceof List ? JsonOutput.toJson(sibling) : sibling}'"
        "(${i + 1},'${text}','${text}',${siblingSql})"
    }.join(",")
    sql "insert into es_source_array_expected values ${arrayRows}"
    def arrayExpectedTable = "internal.${context.dbName}.es_source_array_expected"
    def arrayPredicates = ["limited = 'cat'", "limited in ('cat', 'four')", "limited != 'cat'",
        "limited = '[\"cat\",\"elephant\"]'", "limited is null", "limited is not null",
        "body = 'cat'", "body in ('cat', 'four')", "body != 'cat'", "body is null",
        "limited = 'cat' or body = 'elephant'", "not (limited = 'cat' and body = 'cat')"]
    def siblingPredicates = ["sibling = 'cat'", "sibling in ('cat', 'dog')", "sibling != 'cat'",
        "sibling = '[\"cat\"]'", "sibling is null", "sibling is not null", "sibling <=> null",
        "sibling like '[%'", "sibling not in ('cat')", "not (sibling = 'cat')",
        "sibling = 'cat' or row_id = 1", "sibling = 'cat' and row_id > 0",
        "not (sibling = 'cat' and row_id = 1)"]
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalBatch = sql("show variables like 'batch_size'")[0][1]
    try {
        [false, true].each { docvalues ->
            def catalog = "es_source_safety_${docvalues}"
            sql "drop catalog if exists ${catalog}"
            sql """create catalog ${catalog} properties (
                "type"="es", "hosts"="${endpoint}", "nodes_discovery"="false",
                "enable_keyword_sniff"="true", "enable_docvalue_scan"="${docvalues}"
            )"""
            sql "switch ${catalog}"
            [false, true].each { scannerV2 ->
                sql "set enable_file_scanner_v2 = ${scannerV2}"
                [false, true].each { parallel ->
                    sql "set enable_es_parallel_scroll = ${parallel}"
                    // Small batches exercise residual filtering across pages and prohibit unsafe early LIMIT.
                    sql "set batch_size = 1"
                    def tag = "dv_${docvalues}_v2_${scannerV2}_parallel_${parallel}"
                    quickTest("${tag}_source", "select row_id, limited, normalized, nullable_value, body, copied "
                            + "from doris_source_safety_a order by row_id")
                    predicates.eachWithIndex { predicate, i ->
                        def query = "select row_id from doris_source_safety_a where ${predicate} order by row_id"
                        quickTest("${tag}_predicate_${i}", query,
                                "select row_id from ${expectedTable} where ${predicate} order by row_id")
                        quickTest("${tag}_predicate_${i}", query)
                    }
                    def selectedValueQuery = "select row_id, multi_body, doconly from doris_source_safety_a order by row_id"
                    quickTest("${tag}_selected_values", selectedValueQuery,
                            "select row_id, multi_body, doconly from ${expectedTable} order by row_id")
                    quickTest("${tag}_selected_values", selectedValueQuery)
                    mixedNativePredicates.eachWithIndex { pair, i ->
                        def query = "select row_id from doris_source_safety_a where ${pair[0]} order by row_id"
                        if (!docvalues && i < 3) {
                            // Source STRING OR branches cannot share a local residual with esquery.
                            test {
                                sql query
                                exception "Cannot evaluate esquery locally"
                            }
                        } else {
                            quickTest("${tag}_native_${i}", query,
                                    "select row_id from ${expectedTable} where ${pair[1]} order by row_id")
                            quickTest("${tag}_native_${i}", query)
                        }
                    }
                    ["doris_source_safety_hidden", "doris_source_safety_included"].eachWithIndex { index, i ->
                        def query = "select row_id from ${index} where tenant = 'B' or ${nativeTerm} order by row_id"
                        if (!docvalues) {
                            test {
                                sql query
                                exception "Cannot evaluate esquery locally"
                            }
                        } else {
                            quickTest("${tag}_preserved_source_${i}", query,
                                    "select row_id from ${expectedTable} where tenant = 'B' or tenant = 'A' order by row_id")
                            quickTest("${tag}_preserved_source_${i}", query)
                        }
                    }
                    quickTest("${tag}_arrays", "select row_id, limited, body from doris_source_safety_arrays order by row_id",
                            "select row_id, limited, body from ${arrayExpectedTable} order by row_id")
                    quickTest("${tag}_arrays", "select row_id, limited, body from doris_source_safety_arrays order by row_id")
                    arrayPredicates.eachWithIndex { predicate, i ->
                        def query = "select row_id from doris_source_safety_arrays where ${predicate} order by row_id"
                        quickTest("${tag}_array_predicate_${i}", query,
                                "select row_id from ${arrayExpectedTable} where ${predicate} order by row_id")
                        quickTest("${tag}_array_predicate_${i}", query)
                    }
                    // Projecting limited forces source reads for the entire scan, including sibling.
                    // Compare its STRING predicates to an internal table, not to ES array membership.
                    siblingPredicates.eachWithIndex { predicate, i ->
                        def query = "select row_id, limited, sibling from doris_source_safety_arrays " +
                                "where ${predicate} order by row_id"
                        quickTest("${tag}_sibling_${i}", query,
                                "select row_id, limited, sibling from ${arrayExpectedTable} where ${predicate} order by row_id")
                        quickTest("${tag}_sibling_${i}", query)
                    }
                    def unprojectedSibling = "select row_id, limited from doris_source_safety_arrays " +
                            "where sibling = '[\"cat\"]' order by row_id"
                    quickTest("${tag}_sibling_filter_only", unprojectedSibling,
                            "select row_id, limited from ${arrayExpectedTable} where sibling = '[\"cat\"]' order by row_id")
                    quickTest("${tag}_sibling_filter_only", unprojectedSibling)
                    quickTest("${tag}_sibling_limit", "select row_id, limited, sibling from doris_source_safety_arrays " +
                            "where sibling = 'cat' limit 1")
                    def onlySibling = "select sibling from doris_source_safety_arrays where row_id = 1 and sibling = 'cat'"
                    quickTest("${tag}_sibling_docvalue_control", onlySibling,
                            docvalues ? "select cast('cat' as string)" : "select cast('cat' as string) where false")
                    quickTest("${tag}_sibling_docvalue_control", onlySibling)
                    def nativeSibling = """esquery(sibling, '{"term":{"sibling":"cat"}}')"""
                    def nativeSiblingQuery = "select row_id, limited, sibling from doris_source_safety_arrays " +
                            "where ${nativeSibling} order by row_id"
                    quickTest("${tag}_sibling_native", nativeSiblingQuery,
                            "select row_id, limited, sibling from ${arrayExpectedTable} where row_id in (1,2,3) order by row_id")
                    quickTest("${tag}_sibling_native", nativeSiblingQuery)
                    def siblingAnd = "select row_id, limited, sibling from doris_source_safety_arrays " +
                            "where sibling = 'cat' and ${nativeSibling} order by row_id"
                    quickTest("${tag}_sibling_native_and", siblingAnd,
                            "select row_id, limited, sibling from ${arrayExpectedTable} where sibling = 'cat' order by row_id")
                    quickTest("${tag}_sibling_native_and", siblingAnd)
                    test {
                        sql "select limited, sibling from doris_source_safety_arrays where sibling = 'cat' or ${nativeSibling}"
                        exception "Cannot evaluate esquery locally"
                    }
                    // Native esquery intentionally matches array elements; ordinary SQL above compares
                    // the STRING Doris reads. Keep both contracts explicit instead of guessing scalar shape.
                    def nativeArrayQuery = "select row_id from doris_source_safety_arrays " +
                            "where esquery(limited, '{\"term\":{\"limited\":\"cat\"}}') order by row_id"
                    def nativeArrayIds = request("POST", "doris_source_safety_arrays/_search",
                            [size: 100, query: [term: [limited: "cat"]]]).hits.hits.collect { it._source.row_id }.sort()
                    def nativeArrayOracle = nativeArrayIds.collect {
                        "select cast(${it} as bigint) as row_id"
                    }.join(" union all ")
                    quickTest("${tag}_native_arrays", nativeArrayQuery,
                            "select * from (${nativeArrayOracle}) t order by row_id")
                    quickTest("${tag}_native_arrays", nativeArrayQuery)
                    // These combinations need both source evaluation and remote-only ES semantics.
                    // Reject them while planning rather than dispatching the fake esquery BE function.
                    ["limited = 'elephant' or ${nativeTerm}", "limited = 'cat' or ${nativeTerm}",
                     "limited in ('cat', 'four') or ${nativeTerm}", "body = 'cat' or ${nativeTerm}",
                     "not (limited = 'cat' and ${nativeTerm})", "not (limited = 'elephant' and ${nativeTerm})"].each { predicate ->
                        test {
                            sql "select row_id from doris_source_safety_a where ${predicate}"
                            exception "Cannot evaluate esquery locally"
                        }
                    }
                    ["limited = 'elephant'", "limited is null"].eachWithIndex { predicate, i ->
                        quickTest("${tag}_limit_${i}", "select row_id from doris_source_safety_a "
                                + "where ${predicate} limit 1")
                    }
                    quickTest("${tag}_hidden", "select row_id, limited from doris_source_safety_hidden "
                            + "where limited is null order by row_id")
                    ["doris_source_filtered", "doris_source_multi", "doris_source_routed",
                     "doris_source_filtered_routed", "doris_source_empty", "doris_source_unfiltered",
                     "doris_source_with_empty"].each { alias ->
                        // Compare to ES itself: both Doris scan modes previously shared the same metadata path.
                        def hits = request("POST", "${alias}/_search", [size: 100, _source: ["row_id"],
                                query: [match_all: [:]]]).hits.hits
                        def ids = hits.collect { it._source.row_id }.sort()
                        def oracle = ids.isEmpty() ? "select cast(null as bigint) as row_id where false"
                                : ids.collect { "select cast(${it} as bigint) as row_id" }.join(" union all ")
                        def query = "select row_id from ${alias} order by row_id"
                        quickTest("${tag}_${alias}", query, "select * from (${oracle}) t order by row_id")
                        quickTest("${tag}_${alias}", query)
                        quickTest("${tag}_${alias}_count", "select count(*) from ${alias}")
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set batch_size = ${originalBatch}"
    }
}
