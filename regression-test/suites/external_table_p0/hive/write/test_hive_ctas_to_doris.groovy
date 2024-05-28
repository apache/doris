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

suite("test_hive_ctas_to_doris", "p0,external,hive,external_docker,external_docker_hive") {

    for (String hivePrefix : ["hive2"]) {

        String enabled = context.config.otherConfigs.get("enableHiveTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("diable Hive test.")
            return;
        }

        setHivePrefix(hivePrefix)
        try {
            String str1 = "0123456789" * 6554   // string.length() = 65540
            String str2 = str1.substring(0, 65533)

            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog = "test_hive_ctas_to_doris"
            String hive_tb = "tb_test_hive_ctas_to_doris"
            String db_name = "db_test_hive_ctas_to_doris"

            // create hive table
            sql """drop catalog if exists ${catalog}"""
            sql """create catalog if not exists ${catalog} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """ create database if not exists ${catalog}.${db_name} """
            sql """ drop table if exists ${catalog}.${db_name}.${hive_tb}"""
            sql """ create table ${catalog}.${db_name}.${hive_tb} 
                (id int, str1 string, str2 string, str3 string) """
            sql """ insert into ${catalog}.${db_name}.${hive_tb} values (1, '${str1}', '${str2}', 'str3') """

            qt_q01 """ select length(str1),length(str2) ,length(str3) from ${catalog}.${db_name}.${hive_tb} """
            qt_q02 """ desc ${catalog}.${db_name}.${hive_tb} """

            sql """ create database if not exists internal.${db_name} """

            // ctas for partition
            sql """ create table internal.${db_name}.${hive_tb}_1 (id,str1,str2,str3) auto partition by list (str3)() properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
            qt_q03 """ select length(str1),length(str2) ,length(str3) from internal.${db_name}.${hive_tb}_1 """
            qt_q04 """ desc internal.${db_name}.${hive_tb}_1 """
            sql """ drop table internal.${db_name}.${hive_tb}_1 """

            // ctas for distribution
            sql """ create table internal.${db_name}.${hive_tb}_2 (id,str1,str2,str3) DISTRIBUTED BY HASH(`str2`) BUCKETS 1 properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
            qt_q05 """ select length(str1),length(str2) ,length(str3) from internal.${db_name}.${hive_tb}_2 """
            qt_q06 """ desc internal.${db_name}.${hive_tb}_2 """
            sql """ drop table internal.${db_name}.${hive_tb}_2 """

            // ctas for distribution
            sql """ create table internal.${db_name}.${hive_tb}_3 (id,str1,str2,str3) DISTRIBUTED BY HASH(`str3`) BUCKETS 1 properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
            qt_q07 """ select length(str1),length(str2) ,length(str3) from internal.${db_name}.${hive_tb}_3 """
            qt_q08 """ desc internal.${db_name}.${hive_tb}_3 """
            sql """ drop table internal.${db_name}.${hive_tb}_3 """

            try {
                sql """ create table internal.${db_name}.${hive_tb}_4 (id,str2,str3,str1) auto partition by list (str1)() properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
                assertTrue(false)
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("Partition name's length is over limit of 50"))
            }

            try {
                sql """ create table internal.${db_name}.${hive_tb}_5 (id,str1,str3,str2) auto partition by list (str2)() properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
                assertTrue(false)
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("Partition name's length is over limit of 50"))
            }

            try {
                sql """ create table internal.${db_name}.${hive_tb}_6 (id,str1,str2,str3) DISTRIBUTED BY HASH(`str1`) BUCKETS 1 properties("replication_num" = "1") as select id, str1, str2, str3 from ${catalog}.${db_name}.${hive_tb} """
                assertTrue(false)
            } catch (Exception ex) {
                assertTrue(ex.getMessage().contains("Insert has filtered data in strict mode"))
            }

        } finally {
        }
    }
}

