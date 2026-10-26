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
    try {
        // Small batches force scroll pagination and repeated output block production.
        sql "set batch_size = 1"
        [5, 6, 7, 8].each { version ->
            String port = context.config.otherConfigs.get("es_${version}_port")
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
                    "select test1 from ${timeIndex} where substring(test2, 2) = 'ext2' order by test1",
                    "select test1 from test1 where test1 like 'string%' order by test1",
                    "select count(*), count(message) from test1",
                    "select test1 from test1 where message is null order by test1",
                    "select test1 from test1 where test1 = 'missing-value'",
                    "select test1 from test1 order by test1 limit 2",
                    "select count(*) from (select * from test1 limit 2) t",
                    "select * from composite_type_array order by name"
                ]
                // Compare against the existing scanner on the same index and ES version instead
                // of embedding fixture results that can vary across the external Docker images.
                queries.each { query ->
                    sql "set enable_file_scanner_v2 = false"
                    def expected = sql(query)
                    sql "set enable_file_scanner_v2 = true"
                    def actual = sql(query)
                    assertEquals(expected, actual, "ES ${version}, docvalues=${docvalues}: ${query}")
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${originalV2}"
        sql "set batch_size = ${originalBatchSize}"
    }
}
