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

suite("test_hdfs_binary_mapping", "p0,external,hive,tvf,external_docker") {
    if (!context.config.otherConfigs.get("enableHiveTest")?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("hive2HdfsPort")
    def scanner = sql("SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]
    String target = "binary_mapping_copy"
    try {
        sql "SET enable_file_scanner_v2=true"
        for (def file : [["orc", "test_hdfs_orc/group6/orc_test_binary_column.orc"],
                ["parquet", "test_hdfs_parquet/group6/fixed_len_byte_array.parquet"]]) {
            // UUID annotations retain a separate explicit override; compare the ordinary binary leaf.
            String projection = file[0] == "parquet" ? "decimal_flba, `interval`" : "*"
            def expectedSchema = null
            def expectedRows = null
            for (String option : ["", ', "enable_mapping_varbinary"="false"',
                    ', "enable_mapping_varbinary"="true"']) {
                String tvf = """HDFS("uri"="hdfs://${host}:${port}/user/doris/tvf_data/${file[1]}",
                    "hadoop.username"="doris", "format"="${file[0]}" ${option})"""
                def schema = sql("DESC FUNCTION ${tvf}").findAll { it[0] != "uuid" }
                assertTrue(schema.any { it[1].toString().toLowerCase().startsWith("varbinary") })
                def rows = sql("SELECT ${projection} FROM ${tvf} LIMIT 5")
                // The legacy option must change neither the inferred type nor the bytes returned.
                if (expectedSchema == null) {
                    expectedSchema = schema
                    expectedRows = rows
                } else {
                    assertEquals(expectedSchema, schema)
                    assertEquals(expectedRows, rows)
                }
                sql "DROP TABLE IF EXISTS ${target}"
                // Internal tables do not support VARBINARY; CTAS must not silently store these bytes as TEXT.
                test {
                    sql """CREATE TABLE ${target} DUPLICATE KEY(mapping_key)
                        DISTRIBUTED BY HASH(mapping_key) BUCKETS 1
                        PROPERTIES("replication_num"="1") AS SELECT 1 AS mapping_key, ${projection} FROM ${tvf} LIMIT 5"""
                    exception "doris do not support varbinary create table"
                }
            }
        }
    } finally {
        sql "DROP TABLE IF EXISTS ${target}"
        sql "SET enable_file_scanner_v2=${scanner}"
    }
}
