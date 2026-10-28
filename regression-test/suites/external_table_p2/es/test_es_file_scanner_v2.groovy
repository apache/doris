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

suite("test_es_file_scanner_v2", "p2,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def originalV2 = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def originalBatchSize = sql("show variables like 'batch_size'")[0][1]
    def originalParallelScroll = sql("show variables like 'enable_es_parallel_scroll'")[0][1]
    try {
        [5, 6, 7, 8].each { version ->
            // Properties.get(Object) does not coerce GString keys to the stored String keys.
            String port = context.config.otherConfigs.get("es_${version}_port".toString())
            assertTrue(port != null && !port.trim().isEmpty(), "Missing ES ${version} port configuration")
            String timeIndex = version <= 6 ? "test2_20220808" : "test2"
            [false, true].each { docvalues ->
                String catalog = "test_es_v2_${version}_${docvalues}"
                sql "drop catalog if exists ${catalog}"
                sql """create catalog ${catalog} properties (
                    "type" = "es",
                    "hosts" = "http://${externalEnvIp}:${port}",
                    "nodes_discovery" = "false",
                    "enable_keyword_sniff" = "true",
                    "mapping_es_id" = "true",
                    "enable_docvalue_scan" = "${docvalues}"
                )"""
                sql "switch ${catalog}"
                def queries = [
                    "select test2, test1, test6 from test1 order by test1, test2, test6",
                    "select _id, test1 from test1 order by _id",
                    "select c_bool, c_integer, c_keyword, c_person from test1 order by test1",
                    "select c_byte, c_short, c_long, c_float, c_half_float, c_double, c_scaled_float from test1 order by test1",
                    "select c_date, c_datetime, c_text, c_ip, c_user from test1 order by test1",
                    "select test1 from ${timeIndex} where substring(test2, 2) = 'ext2' order by test1",
                    "select test1 from ${timeIndex} where test4 >= '2022-08-08 00:00:00' and test4 < '2022-08-09 00:00:00' order by test1",
                    "select test1 from test1 where length(test2) > 4 order by test1",
                    "select test1 from test1 where length(test2) > 1000 order by test1",
                    "select test1 from test1 where test1 in ('string1', 'string3') order by test1",
                    "select test1 from test1 where test1 not in ('string1', 'string3') order by test1",
                    "select test1 from test1 where test1 = 'string1' or length(test2) > 5 order by test1",
                    "select test1 from test1 where test1 like 'string%' order by test1",
                    """select test1 from test1 where esquery(test2, '{"match":{"test2":"text#1"}}') order by test1""",
                    "select count(*), count(message) from test1",
                    "select count(*) from test1 where length(test2) > 4",
                    "select test1 from test1 where message is null order by test1",
                    "select test1 from test1 where message is not null order by test1",
                    "select test1 from test1 where test1 = 'missing-value'",
                    "select count(*) from test1 where test1 = 'missing-value'",
                    "select test1 from test1 order by test1 limit 2",
                    "select test1 from test1 where length(test2) > 4 order by test1 limit 2",
                    // Unordered LIMIT may choose different documents on different BEs; compare its cardinality.
                    "select count(*) from (select * from test1 limit 1) t",
                    "select count(*) from (select * from test1 limit 2) t",
                    "select * from composite_type_array order by name"
                ]
                if (version >= 7) {
                    queries.add("select id, name, tags, emails, single_value, description from test_keyword_array order by id")
                }
                if (version == 7) {
                    queries.add("select extra from test_flatten order by extra")
                }
                // Compare against the existing scanner on the same index and ES version instead
                // of embedding fixture results that can vary across the external Docker images.
                [false, true].each { parallelScroll ->
                    sql "set enable_es_parallel_scroll = ${parallelScroll}"
                    // Exercise scroll pagination and the one-request LIMIT path at their batch boundaries.
                    [1, 1024].each { batchSize ->
                        sql "set batch_size = ${batchSize}"
                        queries.each { query ->
                            sql "set enable_file_scanner_v2 = false"
                            def expected = sql(query)
                            sql "set enable_file_scanner_v2 = true"
                            def actual = sql(query)
                            assertEquals(expected, actual,
                                    "ES ${version}, docvalues=${docvalues}, parallel=${parallelScroll}, batch=${batchSize}: ${query}")
                        }
                        // Both scanners share FE pushdown, so use an independent predicate as the oracle.
                        def expectedOr = sql("select test1 from test1 where test1 = 'string2' order by test1")
                        [false, true].each { scannerV2 ->
                            sql "set enable_file_scanner_v2 = ${scannerV2}"
                            def actualOr = sql("""select test1 from test1
                                where (test1 = 'string1' and length(test2) < 0) or test1 = 'string2'
                                order by test1""")
                            assertEquals(expectedOr, actualOr,
                                    "ES ${version}, docvalues=${docvalues}, parallel=${parallelScroll}, batch=${batchSize}, v2=${scannerV2}: nested residual OR")
                        }
                    }
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set batch_size = ${originalBatchSize}"
        sql "set enable_es_parallel_scroll = ${originalParallelScroll}"
    }
}
