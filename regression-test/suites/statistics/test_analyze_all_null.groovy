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

suite("test_analyze_all_null") {

    def wait_row_count_reported = { db, table, row, column, expected ->
        def result = sql """show frontends;"""
        logger.info("show frontends result origin: " + result)
        def host
        def port
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "true") {
                host = result[i][1]
                port = result[i][4]
            }
        }
        def tokens = context.config.jdbcUrl.split('/')
        def url=tokens[0] + "//" + host + ":" + port
        logger.info("Master url is " + url)
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql """use ${db}"""
            result = sql """show frontends;"""
            logger.info("show frontends result master: " + result)
            for (int i = 0; i < 120; i++) {
                Thread.sleep(5000)
                result = sql """SHOW DATA FROM ${table};"""
                logger.info("result " + result)
                if (result[row][column] == expected) {
                    return;
                }
            }
            throw new Exception("Row count report timeout.")
        }

    }

    sql """drop database if exists test_analyze_all_null"""
    sql """create database test_analyze_all_null"""
    sql """use test_analyze_all_null"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE allnull (
            key1 int NULL,
            value1 varchar(25) NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """insert into allnull select null, null from numbers("number"="10000000")"""
    wait_row_count_reported("test_analyze_all_null", "allnull", 0, 4, "10000000")
    sql """analyze table allnull with sample rows 4000000 with sync"""

    def result = sql """show column stats allnull(key1)"""
    assertEquals(1, result.size())
    assertEquals("1.0E7", result[0][2])
    assertEquals("0.0", result[0][3])
    result = sql """show column stats allnull(value1)"""
    assertEquals(1, result.size())
    assertEquals("1.0E7", result[0][2])
    assertEquals("0.0", result[0][3])

    sql """CREATE TABLE invalidTest (
            col1 int NULL,
            col2 string NULL,
            col3 string NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`col1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`col1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """insert into invalidTest values(1, "1", "1")"""

    sql """alter table invalidTest modify column col1 set stats ('row_count'='100', 'ndv'='100', 'num_nulls'='0.0', 'data_size'='3.2E8', 'min_value'='1', 'max_value'='20000000');"""
    sql """alter table invalidTest modify column col2 set stats ('row_count'='100', 'ndv'='0', 'num_nulls'='0.0', 'data_size'='3.2E8', 'min_value'='min', 'max_value'='max');"""
    sql """alter table invalidTest modify column col3 set stats ('row_count'='100', 'ndv'='0', 'num_nulls'='100', 'data_size'='3.2E8', 'min_value'='min', 'max_value'='max');"""
    result = sql """show column cached stats invalidTest"""
    assertEquals(3, result.size())

    explain {
        sql("memo plan select * from invalidTest")
        contains "col1#0 -> ndv=100.0000"
        contains "col2#1 -> ndv=0.0000"
        contains "col3#2 -> ndv=0.0000"
    }

    sql """drop database if exists test_analyze_all_null"""
}
