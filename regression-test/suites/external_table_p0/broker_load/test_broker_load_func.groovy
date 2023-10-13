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

suite("test_broker_load_func", "p0,external,hive,external_docker,external_docker_hive,external_docker_broker") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hdfsPort = context.config.otherConfigs.get("hdfs_port")

            String database_name = "test_broker_load_func"
            String broker_name = "hdfs"

            sql """drop database if exists ${database_name}; """
            sql """create database if not exists ${database_name};"""
            sql """use ${database_name}; """

            sql """
                create table simple (
                    `t_empty_string`  varchar(255) NULL COMMENT '',
                    `t_string` varchar(255) NULL COMMENT ''
                ) engine=olap
                distributed by hash(t_empty_string) buckets 1
                properties (
                    "replication_num" = "1"
                );
                """

            sql """
                LOAD LABEL ${database_name}.label_test_broker_load_func
                (
                    DATA INFILE("hdfs://${externalEnvIp}:${hdfsPort}/user/doris/preinstalled_data/csv/csv_all_types/csv_all_types")
                    INTO TABLE `simple`
                    COLUMNS TERMINATED BY ","
                )
                WITH BROKER ${broker_name}
                (
                     "username"="",
                     "password"=""
                );
            """

            sleep(10000)
            
            def res = sql """select count(*) from simple;"""
            
            assertEquals(10,res[0][0])

        } finally {
        }
    }
}
