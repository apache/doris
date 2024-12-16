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

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        // prepare data in hive
        def hive_database = "test_hive_limit_partition_mtmv_db"
        def hive_table = "partition2"

        def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
        def drop_database_str = """ drop database if exists ${hive_database}"""
        def create_database_str = """ create database ${hive_database}"""
        def create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                        `k1` int)
                                        PARTITIONED BY (
                                        `region` string,
                                        `day` string
                                        )
                                        STORED AS ORC;
                                """
        def add_partition_str = """
                                    alter table ${hive_database}.${hive_table} add if not exists
                                    partition(region="bj",day="20380101")
                                    partition(region="sh",day="20380101")
                                    partition(region="bj",day="20200101")
                                    partition(region="sh",day="20200101")
                                    partition(region="bj",day="20380102")
                                """
        def insert_str1 = """insert into ${hive_database}.${hive_table} PARTITION(region="bj",day="20380101") values(1)"""
        def insert_str2 = """insert into ${hive_database}.${hive_table} PARTITION(region="sh",day="20380101") values(2)"""
        def insert_str3 = """insert into ${hive_database}.${hive_table} PARTITION(region="bj",day="20200101") values(3)"""
        def insert_str4 = """insert into ${hive_database}.${hive_table} PARTITION(region="sh",day="20200101") values(4)"""
        def insert_str5 = """insert into ${hive_database}.${hive_table} PARTITION(region="bj",day="20380102") values(5)"""

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
        logger.info("hive sql: " + insert_str5)
        hive_docker """ ${insert_str5} """

        // prepare catalog
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_${hivePrefix}_limit_partition_mtmv_catalog"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        order_qt_select_base_table "SELECT * FROM ${catalog_name}.${hive_database}.${hive_table}"

        // string type
        def mvName = "test_hive_limit_partition_mtmv"
        def dbName = "regression_test_mtmv_p0"
        sql """drop materialized view if exists ${mvName};"""
        sql """REFRESH catalog ${catalog_name}"""
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
        assertEquals(2, showPartitionsResult.size())
        assertTrue(showPartitionsResult.toString().contains("p_20380101"))
        assertTrue(showPartitionsResult.toString().contains("p_20380102"))

    // date trunc
    sql """drop materialized view if exists ${mvName};"""
    test {
          sql """
              CREATE MATERIALIZED VIEW ${mvName}
              BUILD DEFERRED REFRESH AUTO ON MANUAL
              partition by (date_trunc(`day`,'month'))
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
          exception "only support"
      }

    // date type
    sql """drop materialized view if exists ${mvName};"""
    create_table_str = """ CREATE TABLE ${hive_database}.${hive_table} (
                                `k1` int)
                                PARTITIONED BY (
                                `region` string,
                                `day` date
                                )
                                STORED AS ORC;
                        """
    add_partition_str = """
                            alter table ${hive_database}.${hive_table} add if not exists
                            partition(region="bj",day="2038-01-01")
                            partition(region="sh",day="2038-01-01")
                            partition(region="bj",day="2020-01-01")
                            partition(region="sh",day="2020-01-01")
                        """
    logger.info("hive sql: " + drop_table_str)
    hive_docker """ ${drop_table_str} """
    logger.info("hive sql: " + create_table_str)
    hive_docker """ ${create_table_str} """
    logger.info("hive sql: " + add_partition_str)
    hive_docker """ ${add_partition_str} """

    sql """REFRESH catalog ${catalog_name}"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`day`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
                        'replication_num' = '1',
                        'partition_sync_limit'='2',
                        'partition_sync_time_unit'='YEAR'
                        )
            AS
            SELECT k1,day,region FROM ${catalog_name}.${hive_database}.${hive_table};
        """
    showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertEquals(1, showPartitionsResult.size())
    assertTrue(showPartitionsResult.toString().contains("p_20380101"))
    sql """drop materialized view if exists ${mvName};"""
    sql """drop catalog if exists ${catalog_name}"""
    }
}

