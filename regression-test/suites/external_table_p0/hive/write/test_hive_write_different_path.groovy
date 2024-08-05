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

suite("test_hive_write_different_path", "p0,external,hive,external_docker,external_docker_hive") {

    for (String hivePrefix : ["hive2", "hive3"]) {

        String enabled = context.config.otherConfigs.get("enableHiveTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("diable Hive test.")
            return;
        }

        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port2 = context.config.otherConfigs.get("hive2HdfsPort")
            String hdfs_port3 = context.config.otherConfigs.get("hive3HdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            String catalog1 = "test_${hivePrefix}_write_insert_without_defaultfs"
            String catalog2 = "test_${hivePrefix}_write_insert_with_hdfs2"
            String catalog3 = "test_${hivePrefix}_write_insert_with_hdfs3"

            sql """drop catalog if exists ${catalog1}"""
            sql """drop catalog if exists ${catalog2}"""
            sql """drop catalog if exists ${catalog3}"""
            sql """create catalog if not exists ${catalog1} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'use_meta_cache' = 'true'
            );"""

            sql """ use ${catalog1}.write_test """
            sql """ drop table if exists tb_with_hdfs2 """
            sql """ drop table if exists tb_with_hdfs3 """

            sql """
              CREATE TABLE `tb_with_hdfs2`
              (
                `col_bigint_undef_signed` BIGINT NULL,
                `col_varchar_10__undef_signed` VARCHAR(10) NULL,
                `col_varchar_64__undef_signed` VARCHAR(64) NULL,
                `pk` INT NULL ) properties (
                'location' = 'hdfs://${externalEnvIp}:${hdfs_port2}/user/hive/warehouse/write_test.db/tb_with_hdfs2/'
              );
            """

            sql """
              CREATE TABLE `tb_with_hdfs3`
              (
                `col_bigint_undef_signed` BIGINT NULL,
                `col_varchar_10__undef_signed` VARCHAR(10) NULL,
                `col_varchar_64__undef_signed` VARCHAR(64) NULL,
                `pk` INT NULL ) properties (
                'location' = 'hdfs://${externalEnvIp}:${hdfs_port3}/user/hive/warehouse/write_test.db/tb_with_hdfs3/'
              );
            """

            sql """create catalog if not exists ${catalog2} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port2}',
                'use_meta_cache' = 'true'
            );"""
            sql """create catalog if not exists ${catalog3} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port3}'
            );"""

            sql """ insert into ${catalog1}.write_test.tb_with_hdfs2 values (1,'a','a',1) """
            sql """ insert into ${catalog2}.write_test.tb_with_hdfs2 values (2,'b','a',1) """
            sql """ insert into ${catalog3}.write_test.tb_with_hdfs2 values (3,'c','a',1) """
            sql """ insert into ${catalog1}.write_test.tb_with_hdfs3 values (4,'d','a',1) """
            sql """ insert into ${catalog2}.write_test.tb_with_hdfs3 values (5,'e','a',1) """
            sql """ insert into ${catalog3}.write_test.tb_with_hdfs3 values (6,'f','a',1) """

            order_qt_q001 """ select * from ${catalog1}.write_test.tb_with_hdfs2 order by col_bigint_undef_signed """
            order_qt_q002 """ select * from ${catalog1}.write_test.tb_with_hdfs3 order by col_bigint_undef_signed """

            sql """drop table ${catalog1}.write_test.tb_with_hdfs2"""
            sql """drop table ${catalog1}.write_test.tb_with_hdfs3"""

            sql """drop catalog if exists ${catalog1}"""
            sql """drop catalog if exists ${catalog2}"""
            sql """drop catalog if exists ${catalog3}"""

        } finally {
        }
    }
}

