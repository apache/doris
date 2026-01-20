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

suite("test_create_paimon_table", "p0,external,doris,external_docker,external_docker_doris") {
    String catalog_name = "paimon_hms_catalog_test01"

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive2"]) {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
            String warehouse = "${default_fs}/warehouse"

            // 1. test create catalog
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog ${catalog_name} properties (
                'type'='paimon',
                'paimon.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'warehouse' = '${warehouse}'
            );
            """

            // 2. test create database
            sql """switch ${catalog_name}"""
            String db_name = "test_db"
            sql """create database if not exists ${db_name}"""

            // 3. test create table
            sql """use ${db_name}"""
            sql """drop table if exists ${db_name}.test01"""
            sql """
                CREATE TABLE ${db_name}.test01 (
                    id int
                ) engine=paimon;
            """

            sql """drop table if exists ${db_name}.test02"""
            sql """
                CREATE TABLE ${db_name}.test02 (
                    id int
                ) engine=paimon
                properties("primary-key"=id);
            """

            sql """drop table if exists ${db_name}.test03"""
            sql """
                CREATE TABLE ${db_name}.test03 (
                    c0 int,
                    c1 bigint,
                    c2 float,
                    c3 double,
                    c4 string,
                    c5 date,
                    c6 decimal(10,5),
                    c7 datetime
                ) engine=paimon
                properties("primary-key"=c0);
            """

            sql """drop table if exists ${db_name}.test04"""
            sql """
                CREATE TABLE ${db_name}.test04 (
                    c0 int,
                    c1 bigint,
                    c2 float,
                    c3 double,
                    c4 string,
                    c5 date,
                    c6 decimal(10,5),
                    c7 datetime
                ) engine=paimon
                partition by (c1) ()
                properties("primary-key"=c0);
            """
            
            sql """drop table if exists ${db_name}.test05"""
            sql """
                CREATE TABLE ${db_name}.test05 (
                    c0 int,
                    c1 bigint,
                    c2 float,
                    c3 double,
                    c4 string,
                    c5 date,
                    c6 decimal(10,5),
                    c7 datetime
                ) engine=paimon
                properties(
                 'primary-key' = 'c0,c1',
                 'bucket' = '4',
                 'bucket-key' = 'c0,c1');
            """

            sql """ drop table if exists ${db_name}.test01"""
            sql """ drop table if exists ${db_name}.test02"""
            sql """ drop table if exists ${db_name}.test03"""
            sql """ drop table if exists ${db_name}.test04"""
            sql """ drop table if exists ${db_name}.test05"""
            sql """ drop database if exists ${db_name}"""
            sql """DROP CATALOG IF EXISTS ${catalog_name}"""
        }
    }
}

