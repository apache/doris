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

suite("test_hive_default_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    // prepare data in hive
    def hive_database = "mtmv_default_partition_db"
    def hive_table = "test_hive_default_mtmv_t1"

    def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
    def drop_database_str = """ drop database if exists ${hive_database}"""
    def create_database_str = """ create database ${hive_database}"""
    def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                    user_id INT,
                                    num INT
                                )
                                partitioned by(year int )
                                STORED AS ORC;
                            """

    def set_dynamic_partition_str = """ set hive.exec.dynamic.partition=true;"""
    def set_dynamic_partition_mode_str = """ set hive.exec.dynamic.partition.mode=nonstrict;"""
    def insert_str = """ insert into ${hive_database}.${hive_table} PARTITION(year) values(1,1,2020)"""
    def show_partition_str = """show partitions ${hive_database}.${hive_table}"""

    logger.info("hive sql: " + drop_table_str)
    hive_docker """ ${drop_table_str} """
    logger.info("hive sql: " + drop_database_str)
    hive_docker """ ${drop_database_str} """
    logger.info("hive sql: " + create_database_str)
    hive_docker """ ${create_database_str}"""
    logger.info("hive sql: " + create_table_str)
    hive_docker """ ${create_table_str} """
    logger.info("hive sql: " + set_dynamic_partition_str)
    hive_docker """ ${set_dynamic_partition_str} """
    logger.info("hive sql: " + set_dynamic_partition_mode_str)
    hive_docker """ ${set_dynamic_partition_mode_str} """
    logger.info("hive sql: " + insert_str)
    hive_docker """ ${insert_str} """
    logger.info("hive sql: " + show_partition_str)
    def a = hive_docker """ ${show_partition_str} """
    logger.info("result: " + a)

}

