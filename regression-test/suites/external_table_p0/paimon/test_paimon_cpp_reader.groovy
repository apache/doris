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

suite("test_paimon_cpp_reader", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "test_paimon_cpp_reader"
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

        // Force paimon scan to JNI format, then compare jni/cpp reader results.
        sql """set force_jni_scanner=true"""

        sql """set enable_paimon_cpp_reader=false"""
        def jniAllRows = sql """select c1 from complex_all order by c1"""
        def jniFilteredRows = sql """select c1 from complex_all where c1 >= 2 order by c1"""

        sql """set enable_paimon_cpp_reader=true"""
        def cppAllRows = sql """select c1 from complex_all order by c1"""
        def cppFilteredRows = sql """select c1 from complex_all where c1 >= 2 order by c1"""

        assertTrue(cppAllRows.size() > 0)
        assertEquals(jniAllRows.toString(), cppAllRows.toString())
        assertEquals(jniFilteredRows.toString(), cppFilteredRows.toString())
    } finally {
        sql """set enable_paimon_cpp_reader=false"""
        sql """set force_jni_scanner=false"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
