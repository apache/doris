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

suite("test_paimon_reader_parity", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "test_paimon_reader_parity"
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    try {
        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties (
            "type" = "paimon",
            "paimon.catalog.type" = "filesystem",
            "warehouse" = "hdfs://${externalEnvIp}:${hdfsPort}/user/doris/paimon1"
        );"""
        sql """switch ${catalogName}"""
        sql """use db1"""
        def testQueries = [
                """select c1 from complex_all order by c1""",
                """select c1 from complex_all where c1 >= 2 order by c1""",
                """select * from all_table order by c1""",
                """select * from all_table_with_parquet where c13 like '13%' order by c1""",
                """select * from complex_tab order by c1""",
                """select c3['a_test'], c3['b_test'], c3['bbb'], c3['ccc'] from complex_tab order by c3['a_test'], c3['b_test']""",
                """select array_max(c2) c from complex_tab order by c""",
                """select c20[0] c from complex_all order by c""",
                """select * from deletion_vector_orc""",
                """select * from deletion_vector_parquet"""
        ]

        sql """set force_jni_scanner=true"""
        def jniResults = testQueries.collect { query -> sql(query) }

        sql """set force_jni_scanner=false"""
        def nativeResults = testQueries.collect { query -> sql(query) }

        assertTrue(nativeResults[0].size() > 0)
        for (int i = 0; i < testQueries.size(); i++) {
            assertEquals(jniResults[i].toString(), nativeResults[i].toString())
        }
    } finally {
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
