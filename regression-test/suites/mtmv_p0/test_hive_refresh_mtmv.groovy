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

suite("test_hive_refresh_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
     def hive_database = "mtmv_test_db"
    def hive_table = "test_hive_refresh_mtmv_t1"

    def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
    def drop_database_str = """ drop database if exists ${hive_database}"""
    def create_database_str = """ create database ${hive_database}"""
    def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                    user_id INT,
                                    num INT
                                )
                                STORED AS ORC;
                            """
    def insert_str = """ insert into ${hive_database}.${hive_table} values(1,1),(2,2)"""

    hive_docker """ ${drop_table_str} """
    hive_docker """ ${drop_database_str} """
    hive_docker """ ${create_database_str}"""
    hive_docker """ ${create_table_str} """
    hive_docker """ ${insert_str} """

    String hms_port = context.config.otherConfigs.get("hms_port")
    String catalog_name = "hive_test_mtmv"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""

    order_qt_test "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table}"
}

