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

suite("test_mongodb_splits", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoSplitTest"))) {
        return
    }
    String uri = context.config.otherConfigs.get("mongodb_split_uri")
    def original = sql("SELECT @@mongodb_read_concern,@@time_zone,@@mongodb_split_strategy,@@mongodb_split_field")[0]
    def columns = [[name: "seq", path: ["seq"], bson_types: ["long"], doris_type: "BIGINT"]]
    for (int precision = 0; precision <= 6; precision++) {
        columns.add([name: "time_p${precision}", path: ["observed"], bson_types: ["date"],
            doris_type: "DATETIME(${precision})"])
    }
    for (List spec : [[1, 0], [9, 0], [9, 9], [18, 0], [18, 6], [18, 18], [38, 0], [38, 4], [38, 38]]) {
        columns.add([name: "decimal_p${spec[0]}s${spec[1]}", path: ["amount"], bson_types: ["decimal"],
            doris_type: "DECIMAL(${spec[0]},${spec[1]})"])
    }
    def manifest = [:]
    for (String name : ["integer_records", "date_records", "oid_records", "duplicate_records", "empty_records",
            "decimal_records", "double_records", "string_records", "no_index_records", "record_view", "measurements"]) {
        manifest["doris_mongodb_splits.${name}"] = [columns: columns]
    }
    def create = { String catalog, String policy ->
        sql "DROP CATALOG IF EXISTS ${catalog}"
        def properties = ["type": "mongodb", "mongodb.uri": uri, "mongodb.databases": "doris_mongodb_splits",
            "mongodb.schema.mode": "explicit", "mongodb.schema": groovy.json.JsonOutput.toJson(manifest),
            "mongodb.type_mismatch": policy, "mongodb.read_concern": "snapshot"]
        String values = properties.collect { key, value ->
            "'${key}'='${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'"
        }.join(",")
        sql "CREATE CATALOG ${catalog} PROPERTIES (${values})"
    }
    String namespace = "mongodb_splits_regression.doris_mongodb_splits"
    try {
        create("mongodb_splits_regression", "null")
        create("mongodb_splits_strict", "error")
        sql "SET mongodb_read_concern='inherit'"
        sql "SET mongodb_split_strategy='single'"
        sql "SET mongodb_split_field=''"
        for (String name : ["integer_records", "date_records", "oid_records", "duplicate_records", "empty_records"]) {
            String field = name == "oid_records" ? "_id" : "split_key"
            for (String zone : ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]) {
                sql "SET time_zone='${zone}'"
                def baseline = sql("SELECT * FROM ${namespace}.${name} ORDER BY seq")
                for (int count : [1, 2, 3, 8, 32]) {
                    for (boolean enabled : [false, true]) {
                        def parallel = sql """SELECT /*+ SET_VAR(mongodb_split_strategy='range',
                            mongodb_split_field='${field}',mongodb_split_count=${count},
                            enable_mongodb_predicate_pushdown=${enabled},enable_mongodb_projection_pushdown=${enabled}) */
                            * FROM ${namespace}.${name} ORDER BY seq"""
                        assertEquals(baseline, parallel)
                    }
                }
            }
            "qt_count_${name}" """SELECT /*+ SET_VAR(mongodb_split_strategy='range',
                mongodb_split_field='${field}',mongodb_split_count=8) */ count(*),sum(seq)
                FROM ${namespace}.${name}"""
        }
        sql "SET time_zone='UTC'"
        order_qt_precision """SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_split_count=8,
            mongodb_split_field='split_key') */ * FROM ${namespace}.integer_records WHERE seq < 9"""
        for (String condition : ["seq > 0 AND seq < 300", "CAST(seq AS STRING) LIKE '%7%'",
                "time_p6 >= '2024-03-10 09:59:59.999' AND decimal_p38s4 >= 1", "seq < 3 OR seq > 597"]) {
            def baseline = sql "SELECT * FROM ${namespace}.integer_records WHERE ${condition} ORDER BY seq"
            def parallel = sql """SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_split_count=8,
                mongodb_split_field='split_key') */ * FROM ${namespace}.integer_records
                WHERE ${condition} ORDER BY seq"""
            assertEquals(baseline, parallel)
        }
        for (String name : ["decimal_records", "double_records", "string_records", "no_index_records"]) {
            test {
                sql """SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_split_count=8,
                    mongodb_split_field='split_key') */ count(*) FROM ${namespace}.${name}"""
                exception "range planning unavailable"
            }
            "qt_auto_${name}" """SELECT /*+ SET_VAR(mongodb_split_strategy='auto',mongodb_split_count=8,
                mongodb_split_field='split_key') */ count(*),sum(seq) FROM ${namespace}.${name}"""
        }
        for (String name : ["record_view", "measurements"]) {
            test {
                sql """SELECT /*+ SET_VAR(mongodb_read_concern='majority',mongodb_split_strategy='range',
                    mongodb_split_count=8) */ count(*) FROM ${namespace}.${name}"""
                exception "unsupported_object"
            }
        }
        for (String strategy : ["single", "range"]) {
            test {
                sql """SELECT /*+ SET_VAR(mongodb_split_strategy='${strategy}',mongodb_split_count=8,
                    mongodb_split_field='split_key') */ decimal_p1s0
                    FROM mongodb_splits_strict.doris_mongodb_splits.integer_records"""
                exception "BSON"
            }
        }
        test {
            sql """SELECT /*+ SET_VAR(mongodb_read_concern='majority',mongodb_split_strategy='range',
                mongodb_split_count=8) */ count(*) FROM ${namespace}.integer_records"""
            exception "snapshot_required"
        }
        test {
            sql """SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_split_field='split_key',
                mongodb_split_count=8) */ l.seq FROM ${namespace}.integer_records l
                JOIN ${namespace}.date_records r ON l.seq=r.seq"""
            exception "exactly one MongoDB scan"
        }
        String plan = sql("EXPLAIN SELECT /*+ SET_VAR(mongodb_split_strategy='range',mongodb_split_count=8," +
            "mongodb_split_field='split_key') */ count(*) FROM ${namespace}.duplicate_records").toString()
        assertTrue(plan.contains("requested/admitted/actual: 8/8/2"))
        assertTrue(plan.contains("type: integer"))
        qt_statement_restored "SELECT @@mongodb_split_strategy,length(@@mongodb_split_field)"
    } finally {
        sql "SET mongodb_read_concern='${original[0]}'"
        sql "SET time_zone='${original[1]}'"
        sql "SET mongodb_split_strategy='${original[2]}'"
        sql "SET mongodb_split_field='${original[3]}'"
    }
}
