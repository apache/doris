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

suite("test_tvf_binary_mapping", "external,hive,tvf,external_docker") {
    if (!context.config.otherConfigs.get("enableHiveTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("hive2HdfsPort")
    def originalReader = sql("SELECT @@enable_file_scanner_v2")[0][0]
    def fixtures = [
        [format: "orc", path: "test_hdfs_orc/group0/TestOrcFile.test1.orc", column: "bytes1", id: "int1"],
        [format: "parquet", path: "test_hdfs_parquet/group0/alltypes_plain.parquet",
            column: "string_col", id: "id"]
    ]
    try {
        for (boolean readerV2 : [false, true]) {
            sql "SET enable_file_scanner_v2 = ${readerV2}"
            for (def fixture : fixtures) {
                def baseline = null
                for (String legacyValue : ["true", "false", null]) {
                    String legacyProperty = legacyValue == null ? "" :
                            ", \"enable_mapping_varbinary\"=\"${legacyValue}\""
                    String source = """HDFS(
                        "uri"="hdfs://${host}:${port}/user/doris/tvf_data/${fixture.path}",
                        "hadoop.username"="doris", "format"="${fixture.format}"${legacyProperty})"""
                    def schema = sql "DESC FUNCTION ${source}"
                    assertTrue(schema.find { it[0] == fixture.column }[1].toString().startsWith("varbinary"))
                    def rows = sql "SELECT HEX(`${fixture.column}`) FROM ${source} ORDER BY 1"
                    if (baseline == null) {
                        baseline = rows
                    }
                    // Legacy flags must not change the inferred schema or the stored bytes.
                    assertEquals(baseline, rows)
                    if (fixture.format == "orc") {
                        assertEquals([[""], ["0001020304"]], rows)
                    }
                    sql "DROP TABLE IF EXISTS tvf_binary_mapping_ctas"
                    // Internal tables do not support VARBINARY; CTAS must not silently decode it as text.
                    test {
                        sql """CREATE TABLE tvf_binary_mapping_ctas PROPERTIES ("replication_num"="1")
                            AS SELECT `${fixture.id}` AS id, `${fixture.column}` AS payload FROM ${source}"""
                        exception "varbinary"
                    }
                    sql """CREATE TABLE tvf_binary_mapping_ctas PROPERTIES ("replication_num"="1")
                        AS SELECT `${fixture.id}` AS id, HEX(`${fixture.column}`) AS payload_hex FROM ${source}"""
                    assertEquals(rows, sql("SELECT payload_hex FROM tvf_binary_mapping_ctas ORDER BY 1"))
                }
            }
        }
    } finally {
        sql "DROP TABLE IF EXISTS tvf_binary_mapping_ctas"
        sql "SET enable_file_scanner_v2 = ${originalReader}"
    }
}
