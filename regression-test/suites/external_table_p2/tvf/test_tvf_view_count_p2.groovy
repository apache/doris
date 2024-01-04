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

suite("test_tvf_view_count_p2", "p2,external,tvf,external_remote,external_remote_tvf") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hdfsPort = context.config.otherConfigs.get("extHdfsPort")

        sql """drop database if exists test_tvf_view_count_p2"""
        sql """create database test_tvf_view_count_p2"""
        sql """use test_tvf_view_count_p2"""
        sql """set enable_nereids_planner=false"""
        sql """create view tvf_view_count as select * from hdfs (
            "uri"="hdfs://${nameNodeHost}:${hdfsPort}/usr/hive/warehouse/tpch_1000_parquet.db/part/000091_0",
            "hadoop.username" = "hadoop",
            "format"="parquet");"""

        def result = sql """explain verbose select count(1) from tvf_view_count;"""
        def contain0 = false;
        def contain1 = false;
        for (String value : result) {
            if (value.contains("SlotDescriptor{id=0,")) {
                contain0 = true;
            }
            if (value.contains("SlotDescriptor{id=1,")) {
                contain1 = true;
            }
        }
        assertTrue(contain0)
        assertFalse(contain1)

        sql """drop database if exists test_tvf_view_count_p2"""
    }
}

