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

suite("test_hot_value") {

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

    sql """drop database if exists test_hot_value"""
    sql """create database test_hot_value"""
    sql """use test_hot_value"""
    sql """set global enable_auto_analyze=false"""
    sql " set hot_value_threshold = 1"

    sql """CREATE TABLE test1 (
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
    sql """insert into test1 select number, number % 2 from numbers("number"="10000")"""
    sql """CREATE TABLE test2 (
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
    sql """insert into test2 select number, " : ;a" from numbers("number"="10000")"""
    wait_row_count_reported("test_hot_value", "test1", 0, 4, "10000")
    wait_row_count_reported("test_hot_value", "test2", 0, 4, "10000")
    sql """analyze table test1 with sync"""
    explain {
        sql("memo plan select * from test1")
        contains "hotValues=(null)"
    }
    def result = sql """show column stats test1(key1)"""
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    assertEquals("null", result[0][17])
    result = sql """show column stats test1(value1)"""
    logger.info("0. result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    assertEquals("null", result[0][17])
    result = sql """show column cached stats test1(key1)"""
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    assertEquals("null", result[0][17])
    result = sql """show column cached stats test1(value1)"""
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    assertEquals("null", result[0][17])
    sql """drop stats test1"""
    sql """analyze table test1 with sample rows 400 with sync"""
    result = sql """show column stats test1(key1)"""
    logger.info("result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    result = sql """show column stats test1(value1)"""
    logger.info("1. result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    String[] hotValues = result[0][17].split(";")
    logger.info("1.1 hotValues " + result[0][17])
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0] == "'1':0.5" || hotValues[0] == "'0':0.5")
    assertTrue(hotValues[1] == "'1':0.5" || hotValues[1] == "'0':0.5")
    result = sql """show column cached stats test1(value1)"""
    logger.info("result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0] == "'1':0.5" || hotValues[0] == "'0':0.5")
    assertTrue(hotValues[1] == "'1':0.5" || hotValues[1] == "'0':0.5")

    sql """drop stats test1"""
    sql """analyze table test1 with sample rows 40000 with sync"""
    result = sql """show column stats test1(key1)"""
    logger.info("result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    result = sql """show column stats test1(value1)"""
    logger.info("2. result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0] == "'1':50.0" || hotValues[0] == "'0':50.0")
    assertTrue(hotValues[1] == "'1':50.0" || hotValues[1] == "'0':50.0")
    result = sql """show column cached stats test1(value1)"""
    logger.info("result " + result)
    assertEquals(1, result.size())
    assertEquals("10000.0", result[0][2])
    hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0] == "'1':50.0" || hotValues[0] == "'0':50.0")
    assertTrue(hotValues[1] == "'1':50.0" || hotValues[1] == "'0':50.0")

    sql """alter table test1 modify column value1 set stats ('row_count'='5.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='34.0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'hot_values'='aaa :22.33');"""
    result = sql """show column stats test1(value1)"""
    logger.info("3. result " + result)
    assertEquals(1, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("'aaa':22.33", result[0][17])
    result = sql """show column cached stats test1(value1)"""
    assertEquals(1, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("'aaa':22.33", result[0][17])
    explain {
        sql("memo plan select * from test1")
        contains "hotValues=('aaa':22.33)"
    }

    sql """alter table test1 modify column value1 set stats ('row_count'='5.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='34.0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST', 'hot_values'='a \\\\;a \\\\:a :22.33');"""
    result = sql """show column stats test1(value1)"""
    logger.info("4. result " + result)
    assertEquals(1, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("'a ;a :a':22.33", result[0][17])
    result = sql """show column cached stats test1(value1)"""
    assertEquals(1, result.size())
    assertEquals("5.0", result[0][2])
    assertEquals("'a ;a :a':22.33", result[0][17])
    explain {
        sql("memo plan select * from test1")
        contains "hotValues=('a ;a :a':22.33)"
    }

    sql """analyze table test2 with sample rows 100 with sync"""
    result = sql """show column stats test2(value1)"""
    assertEquals(1, result.size())
    assertEquals("' : ;a':100.0", result[0][17])
    result = sql """show column cached stats test2(value1)"""
    assertEquals(1, result.size())
    assertEquals("' : ;a':100.0", result[0][17])

    sql """drop stats test2"""
    sql """analyze table test2 with sample rows 4000000 with sync"""
    result = sql """show column stats test2(value1)"""
    assertEquals(1, result.size())
    assertEquals("' : ;a':100.0", result[0][17])
    result = sql """show column cached stats test2(value1)"""   
    assertEquals(1, result.size())
    assertEquals("' : ;a':100.0", result[0][17])

    sql """drop database if exists test_hot_value"""
}
