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

suite("test_hive_limit_partition_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    // prepare data in hive
    def hive_database = "test_hive_limit_partition_mtmv_db"
    def hive_table = "partition2"

    def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
    def drop_database_str = """ drop database if exists ${hive_database}"""
    def create_database_str = """ create database ${hive_database}"""
    def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                     `k1` int)
                                    PARTITIONED BY (
                                      `day` string,
                                      `region` string)
                                    STORED AS ORC;
                            """
    def add_partition_str = """
                                alter table ${hive_database}.${hive_table} add if not exists
                                partition(day="20380101",region="bj")
                                partition(day="20380101",region="sh")
                                partition(day="20200101",region="bj")
                                partition(day="20200101",region="sh")
                            """
    def insert_str1 = """insert into ${hive_database}.${hive_table} PARTITION(day="20380101",region="bj") values(1)"""
    def insert_str2 = """insert into ${hive_database}.${hive_table} PARTITION(day="20380101",region="sh") values(2)"""
    def insert_str3 = """insert into ${hive_database}.${hive_table} PARTITION(day="20200101",region="bj") values(3)"""
    def insert_str4 = """insert into ${hive_database}.${hive_table} PARTITION(day="20200101",region="sh") values(4)"""

    logger.info("hive sql: " + drop_table_str)
    hive_docker """ ${drop_table_str} """
    logger.info("hive sql: " + drop_database_str)
    hive_docker """ ${drop_database_str} """
    logger.info("hive sql: " + create_database_str)
    hive_docker """ ${create_database_str}"""
    logger.info("hive sql: " + create_table_str)
    hive_docker """ ${create_table_str} """
    logger.info("hive sql: " + add_partition_str)
    hive_docker """ ${add_partition_str} """
    logger.info("hive sql: " + insert_str1)
    hive_docker """ ${insert_str1} """
    logger.info("hive sql: " + insert_str2)
    hive_docker """ ${insert_str2} """
    logger.info("hive sql: " + insert_str3)
    hive_docker """ ${insert_str3} """
    logger.info("hive sql: " + insert_str4)
    hive_docker """ ${insert_str4} """

    // prepare catalog
    String hms_port = context.config.otherConfigs.get("hms_port")
    String catalog_name = "test_hive_limit_partition_mtmv_catalog"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""

    order_qt_select_base_table "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table}"


    // prepare mtmv
    def mvName = "test_hive_limit_partition_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop materialized view if exists ${mvName};"""

    // partition by day
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`day`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                        'replication_num' = '1',
                        'partition_sync_limit'='2',
                        'partition_sync_time_unit'='MONTH',
                        'partition_date_format'='%Y%m%d'
                        )
            AS
            SELECT k1,day,region FROM ${catalog_name}.${hive_database}.${hive_table};
        """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101"))

    // refresh complete
    sql """
         REFRESH MATERIALIZED VIEW ${mvName} complete
     """
     waitingMTMVTaskFinished(jobName)
     order_qt_mtmv_complete "SELECT * FROM ${mvName} order by k1,day,region"

   sql """drop materialized view if exists ${mvName};"""
   sql """drop catalog if exists ${catalog_name}"""
}

