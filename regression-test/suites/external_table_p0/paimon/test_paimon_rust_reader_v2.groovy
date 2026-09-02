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

// Paimon rust reader under FileScannerV2: enable_paimon_rust_reader=true must work with
// the default enable_file_scanner_v2=true (before the v2 port, a rust split was rejected by
// FileScannerV2::is_supported and the reader was only reachable with v2 disabled).
suite("test_paimon_rust_reader_v2", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "test_paimon_rust_reader_v2"
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
        sql """set force_jni_scanner=false"""
        // FileScannerV2 is the default; make the intent of this suite explicit.
        sql """set enable_file_scanner_v2=true"""

        def testQueries = [
                """select c1 from complex_all order by c1""",
                // Filter on a data column exercises the v2 predicate pushdown into the
                // paimon-rust filter (PaimonRustPredicateConverter global-index mode).
                """select c1 from complex_all where c1 >= 2 order by c1""",
                """select * from all_table order by c1""",
                """select * from all_table_with_parquet where c13 like '13%' order by c1""",
                """select * from complex_tab order by c1""",
                """select c3['a_test'], c3['b_test'], c3['bbb'], c3['ccc'] from complex_tab order by c3['a_test'], c3['b_test']""",
                """select array_max(c2) c from complex_tab order by c""",
                """select c20[0] c from complex_all order by c""",
                """select * from deletion_vector_orc""",
                """select * from deletion_vector_parquet""",
                // count(*) exercises the table-level metadata count path of the v2 leaf reader.
                """select count(*) from append_table""",
                """select count(*) from merge_on_read_table""",
                """select count(*) from deletion_vector_parquet""",
                // count(*) with a filter must fall back to the real rust scan.
                """select count(*) from all_table where c1 >= 2""",
                // Full scans of the count-suite tables exercise merge-on-read and
                // deletion-vector batches through the v2 leaf reader.
                """select * from append_table order by product_id""",
                """select * from merge_on_read_table order by product_id""",
                """select * from deletion_vector_parquet order by id"""
        ]

        // Default path is JNI when enable_paimon_rust_reader=false.
        sql """set enable_paimon_rust_reader=false"""
        def jniResults = testQueries.collect { query -> sql(query) }

        sql """set enable_paimon_rust_reader=true"""
        def rustResults = testQueries.collect { query -> sql(query) }

        assertTrue(rustResults[0].size() > 0)
        for (int i = 0; i < testQueries.size(); i++) {
            assertEquals(jniResults[i].toString(), rustResults[i].toString())
        }

        // The v1 fallback path must keep working when v2 is explicitly disabled.
        sql """set enable_file_scanner_v2=false"""
        def v1RustResults = testQueries.collect { query -> sql(query) }
        for (int i = 0; i < testQueries.size(); i++) {
            assertEquals(jniResults[i].toString(), v1RustResults[i].toString())
        }
    } finally {
        sql """set enable_paimon_rust_reader=false"""
        sql """set enable_file_scanner_v2=true"""
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
