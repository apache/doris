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

suite("test_es_source_review_cases", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableEsTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("es_8_port")
    assertTrue(port != null && !port.isEmpty(), "es_8_port must be configured")
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
                connection.outputStream.withCloseable { it.write(JsonOutput.toJson(body).getBytes("UTF-8")) }
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
    request("DELETE", "doris_source_review_cases")
    request("PUT", "doris_source_review_cases", [
        settings: [number_of_shards: 1, number_of_replicas: 0,
            analysis: [normalizer: [folded: [type: "custom", filter: ["lowercase"]]]]],
        mappings: [properties: [row_id: [type: "long"], tag: [type: "keyword", normalizer: "folded"],
            n: [type: "integer"], f: [type: "float"], obj: [type: "flattened"],
            epoch: [type: "date", format: "epoch_millis"], basic: [type: "date", format: "basic_date"]]]])
    [[row_id: 1, tag: "UPPER", n: new BigDecimal("0.9999999999999999"),
        f: new BigDecimal("1.0000000596046448"), obj: [:], epoch: "2024", basic: "20240101"],
     [row_id: 2, tag: "UPPER", n: null, f: null, obj: null, epoch: 2024, basic: 20240101],
     [row_id: 3, tag: "UPPER", n: 1, f: 1.0, obj: [key: "value"], epoch: "0", basic: "20240101"]].each { row ->
        request("PUT", "doris_source_review_cases/_doc/${row.row_id}", row)
    }
    request("POST", "doris_source_review_cases/_refresh")

    sql "switch internal"
    sql "drop table if exists es_source_review_control"
    sql """create table es_source_review_control (row_id bigint, tag string, n int, f float, obj json)
        distributed by hash(row_id) buckets 1 properties("replication_num"="1")"""
    String control = "internal.${context.dbName}.es_source_review_control"
    sql "drop table if exists es_source_review_dates"
    sql """create table es_source_review_dates (row_id bigint, tag string, basic datetimev2(0))
        engine=elasticsearch properties("hosts"="${endpoint}", "index"="doris_source_review_cases",
            "nodes_discovery"="false", "enable_docvalue_scan"="false")"""
    String legacyDates = "internal.${context.dbName}.es_source_review_dates"
    // Legacy ES table metadata is populated by the repository daemon, not by CREATE TABLE.
    awaitUntil(30, 1) {
        try {
            sql "explain select row_id,tag,basic from ${legacyDates}"
            return true
        } catch (Exception error) {
            if (error.message.contains("EsTable metadata has not been synced")) {
                return false
            }
            throw error
        }
    }
    sql "drop catalog if exists es_source_review_catalog"
    sql """create catalog es_source_review_catalog properties("type"="es", "hosts"="${endpoint}",
        "nodes_discovery"="false", "enable_docvalue_scan"="true")"""
    def originalScanner = sql("select @@enable_file_scanner_v2")[0][0]
    def originalParallel = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    def originalZone = sql("select @@time_zone")[0][0]
    try {
        sql "set time_zone = 'UTC'"
        sql "switch es_source_review_catalog"
        // Compare filters with the values Doris actually materializes, independently of ES's
        // numeric indexing conversion. The normalized tag makes this entire projection use source.
        sql """insert into ${control} select row_id,tag,n,f,obj from doris_source_review_cases"""
        [false, true].each { scanner ->
            sql "set enable_file_scanner_v2 = ${scanner}"
            [false, true].each { parallel ->
                sql "set enable_es_parallel_scroll = ${parallel}"
                String tag = "v2_${scanner}_parallel_${parallel}"
                ["n = 1", "n != 1", "n > 0", "n is null", "n = 1 or row_id = 3", "not (n = 1)",
                 "f = cast(1 as float)", "f != cast(1 as float)", "obj is null", "obj is not null"].eachWithIndex {
                    String predicate, int i ->
                    quickTest("${tag}_filter_${i}",
                        "select row_id,tag,n,f from doris_source_review_cases where ${predicate} order by row_id",
                        "select row_id,tag,n,f from ${control} where ${predicate} order by row_id")
                }
                quickTest("${tag}_local_filter_limit",
                    "select row_id,tag from doris_source_review_cases where n = 1 order by row_id limit 1",
                    "select row_id,tag from ${control} where n = 1 order by row_id limit 1")
                // Explicit legacy DATETIMEV2 columns must honor basic_date for strings and numeric tokens.
                quickTest("${tag}_legacy_basic_date",
                    "select row_id,tag,basic from ${legacyDates} order by row_id",
                    "select row_id,tag,cast('2024-01-01' as datetimev2(0)) from ${control} order by row_id")
                // epoch_millis-only accepts both the string and numeric 2024 as milliseconds, not a year.
                quickTest("${tag}_epoch_format",
                    "select row_id,tag,cast(epoch as date) from doris_source_review_cases order by row_id",
                    "select row_id,tag,cast('1970-01-01' as date) from ${control} order by row_id")
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalScanner}"
        sql "set enable_es_parallel_scroll = ${originalParallel}"
        sql "set time_zone = '${originalZone}'"
        sql "switch internal"
    }
}
