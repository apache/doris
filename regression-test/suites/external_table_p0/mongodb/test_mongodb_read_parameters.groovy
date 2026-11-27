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

suite("test_mongodb_read_parameters", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoReadParameterTest"))) {
        return
    }
    def quote = { Object value -> "'${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'" }
    def manifest = groovy.json.JsonOutput.toJson([
        "doris_mongodb_splits.integer_records": [columns: [
            [name: "seq", path: ["seq"], bson_types: ["long"], doris_type: "BIGINT"]]]])
    def expected = (0..<16).collect { [it as Long] }
    def original = sql("SELECT @@mongodb_read_preference,@@mongodb_read_concern,@@mongodb_split_strategy")[0]
    try {
        sql "SET mongodb_read_preference='inherit'"
        sql "SET mongodb_read_concern='inherit'"
        sql "SET mongodb_split_strategy='single'"
        for (String topology : ["replica", "mongos"]) {
            // Config keys are Java Strings; a GString has a different hash code.
            String uri = context.config.otherConfigs.get("mongodb_${topology}_uri".toString())
            assertTrue(uri != null && !uri.isEmpty(),
                    "Start the MongoDB CI fixture before running enabled read-parameter tests")
            String catalog = "mongodb_parameters_${topology}"
            def create = { Map overrides ->
                sql "DROP CATALOG IF EXISTS ${catalog}"
                def properties = ["type": "mongodb", "mongodb.uri": uri,
                    "mongodb.databases": "doris_mongodb_splits", "mongodb.schema.mode": "explicit",
                    "mongodb.schema": manifest, "mongodb.type_mismatch": "null",
                    "mongodb.read_preference": "secondary", "mongodb.read_concern": "snapshot",
                    "mongodb.datetime_timezone": "UTC", "mongodb.pool_max_clients": "2"]
                properties.putAll(overrides)
                sql "CREATE CATALOG ${catalog} PROPERTIES (" + properties.collect { k, v ->
                    "${quote(k)}=${quote(v)}"
                }.join(",") + ")"
            }
            String table = "${catalog}.doris_mongodb_splits.integer_records"
            try {
                create([:])
                // This fixture has real secondaries, unlike the standalone read-policy suite.
                assertEquals(expected, sql("SELECT seq FROM ${table} WHERE seq<16 ORDER BY seq"))
                for (boolean streaming : [false, true]) {
                    for (String strategy : ["single", "auto", "range"]) {
                        for (String concern : ["local", "majority", "available", "snapshot"]) {
                            for (String preference : ["primary", "primaryPreferred", "secondary",
                                    "secondaryPreferred", "nearest"]) {
                                String options = "mongodb_read_preference='${preference}'," +
                                    "mongodb_read_concern='${concern}',mongodb_split_strategy='${strategy}'," +
                                    "file_split_size=1,max_file_split_num=8,batch_size=16," +
                                    "enable_external_table_batch_mode=${streaming}"
                                String query = "SELECT /*+ SET_VAR(${options}) */ seq FROM ${table} WHERE seq<16 ORDER BY seq"
                                if (strategy == "range" && concern != "snapshot") {
                                    test {
                                        sql query
                                        exception "snapshot_required"
                                    }
                                } else {
                                    assertEquals(expected, sql(query))
                                }
                            }
                        }
                    }
                }
                for (int batch : [1, 16, 1024, 65535]) {
                    assertEquals(expected, sql("SELECT /*+ SET_VAR(batch_size=${batch}) */ seq FROM ${table} WHERE seq<16 ORDER BY seq"))
                }
                for (boolean enabled : [false, true]) {
                    assertEquals(expected, sql("""SELECT /*+ SET_VAR(enable_mongodb_predicate_pushdown=${enabled},
                        enable_mongodb_like_pushdown=${enabled},enable_mongodb_projection_pushdown=${enabled}) */
                        seq FROM ${table} WHERE seq<16 AND CAST(seq AS STRING) LIKE '%' ORDER BY seq"""))
                }
                for (String option : ["mongodb_split_strategy='invalid'", "mongodb_read_concern='linearizable'",
                        "mongodb_read_preference='invalid'", "mongodb_split_field='a.b'"]) {
                    test {
                        sql "SELECT /*+ SET_VAR(${option}) */ seq FROM ${table}"
                        check { result, error, startTime, endTime ->
                            assertTrue(error != null && error.toString().toLowerCase().contains("mongo"))
                        }
                    }
                }
                for (int pool : [1, 1024]) {
                    create(["mongodb.pool_max_clients": pool.toString()])
                    assertEquals(expected, sql("SELECT seq FROM ${table} WHERE seq<16 ORDER BY seq"))
                }
                for (String key : ["mongodb.connect_timeout_ms", "mongodb.server_selection_timeout_ms",
                        "mongodb.socket_timeout_ms"]) {
                    for (String invalid : ["0", "-1", "300001"]) {
                        test {
                            sql "CREATE CATALOG ${catalog}_invalid PROPERTIES ('type'='mongodb','mongodb.uri'=${quote(uri)},'${key}'='${invalid}')"
                            exception key
                        }
                    }
                }
                for (String invalid : ["0", "-1", "1025"]) {
                    test {
                        sql "CREATE CATALOG ${catalog}_invalid PROPERTIES ('type'='mongodb','mongodb.uri'=${quote(uri)},'mongodb.pool_max_clients'='${invalid}')"
                        exception "mongodb.pool_max_clients"
                    }
                }
            } finally {
                sql "DROP CATALOG IF EXISTS ${catalog}"
                sql "DROP CATALOG IF EXISTS ${catalog}_invalid"
            }
        }
        assertEquals([["inherit", "inherit", "single"]], sql("SELECT @@mongodb_read_preference,@@mongodb_read_concern,@@mongodb_split_strategy"))
    } finally {
        sql "SET mongodb_read_preference=${quote(original[0])}"
        sql "SET mongodb_read_concern=${quote(original[1])}"
        sql "SET mongodb_split_strategy=${quote(original[2])}"
    }
}
