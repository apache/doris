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

suite("test_hive_insert_overwrite_with_empty_table", "p0,external,hive,external_docker,external_docker_hive") {

    for (String hivePrefix : ["hive2"]) {

        String enabled = context.config.otherConfigs.get("enableHiveTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("disable Hive test.")
            return;
        }

        setHivePrefix(hivePrefix)
        try {

            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog = "test_hive_insert_overwrite_with_empty_table"
            String db1 = catalog + "_db"
            String tb1 = db1 + "_tb1"
            String tb2 = db1 + "_tb2"
            String tb3 = db1 + "_tb3"

            sql """drop catalog if exists ${catalog}"""
            sql """create catalog if not exists ${catalog} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            sql """ use ${catalog}.${db1} """

            sql """ drop table if exists ${db1}.${tb1} """
            sql """ drop table if exists ${db1}.${tb2} """
            sql """ drop database if exists ${db1} """

            sql """ create database ${db1} """
            sql """ create table ${db1}.${tb1} (id int, val int) partition by list (val)() """
            sql """ create table ${db1}.${tb2} (id int, val int) """
            sql """ create table ${db1}.${tb3} (id int, val int) """

            sql """ use ${db1} """
            sql """ insert into ${tb1} values (1,1), (1,2), (1,3) """
            sql """ insert into ${tb2} values (1,1), (1,2), (1,3) """

            order_qt_q0 """ select * from ${tb1} """
            order_qt_q1 """ select * from ${tb2} """

            sql """ insert overwrite table ${tb1} select * from ${tb3} """
            sql """ insert overwrite table ${tb2} select * from ${tb3} """

            order_qt_q2 """ select * from ${tb1} """   // should have 3 records
            order_qt_q3 """ select * from ${tb2} """   // should have no records

            sql """ drop table ${db1}.${tb1} """
            sql """ drop table ${db1}.${tb2} """
            sql """ drop table ${db1}.${tb3} """
            sql """ drop database ${db1} """
            sql """ drop catalog ${catalog} """

        } finally {
        }
    }
}

