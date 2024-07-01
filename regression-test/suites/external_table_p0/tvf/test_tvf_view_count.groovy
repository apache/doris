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

suite("test_tvf_view_count", "p0,external,tvf,external_docker,hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("externalEnvIp")
        String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")

        sql """drop database if exists test_tvf_view_count_p2"""
        sql """create database test_tvf_view_count_p2"""
        sql """use test_tvf_view_count_p2"""
        sql """set enable_nereids_planner=false"""
        sql """create view tvf_view_count as select * from hdfs (
            "uri"="hdfs://${nameNodeHost}:${hdfsPort}/user/doris/tpch1.db/tpch1_parquet/part/part-00000-cb9099f7-a053-4f9a-80af-c659cfa947cc-c000.snappy.parquet",
            "hadoop.username" = "hadoop",
            "format"="parquet");"""

        explain {
            verbose true
            sql("select count(1) from tvf_view_count")
            contains "SlotDescriptor{id=0,"
            notContains "SlotDescriptor{id=1,"
        }

        sql """drop database if exists test_tvf_view_count_p2"""
    }
}

