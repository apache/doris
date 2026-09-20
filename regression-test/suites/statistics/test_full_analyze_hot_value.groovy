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

suite("test_full_analyze_hot_value") {

    sql """drop database if exists test_full_analyze_hot_value"""
    sql """create database test_full_analyze_hot_value"""
    sql """use test_full_analyze_hot_value"""
    sql """set global enable_auto_analyze=false"""

    // Test 1: Full analyze collects hot_value for varchar column with skewed distribution
    sql """drop table if exists full_hot_skew"""
    sql """CREATE TABLE full_hot_skew (
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
    // Insert 100 rows: value1 has 2 values, "0" and "1", each appearing 50 times
    sql """insert into full_hot_skew select number, number % 2 from numbers("number"="100")"""

    sql """analyze table full_hot_skew with sync"""
    def result = sql """show column stats full_hot_skew(value1)"""
    logger.info("Test1 result: " + result)
    assertEquals(1, result.size())
    assertEquals("100.0", result[0][2])
    // Full analyze should now collect hot_value
    assertTrue(result[0][17].contains(":"), "Full analyze should collect hot_value, but got " + result[0][17])
    String[] hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0].trim() == "'1':0.5" || hotValues[0].trim() == "'0':0.5")
    assertTrue(hotValues[1].trim() == "'1':0.5" || hotValues[1].trim() == "'0':0.5")

    // Verify cached stats also have hot_value
    result = sql """show column cached stats full_hot_skew(value1)"""
    logger.info("Test1 cached result: " + result)
    assertEquals(1, result.size())
    hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0].trim() == "'1':0.5" || hotValues[0].trim() == "'0':0.5")
    assertTrue(hotValues[1].trim() == "'1':0.5" || hotValues[1].trim() == "'0':0.5")

    // Test 2: Full analyze collects hot_value for int column
    result = sql """show column stats full_hot_skew(key1)"""
    logger.info("Test2 result: " + result)
    assertEquals(1, result.size())
    assertEquals("100.0", result[0][2])
    // key1 has 100 unique values, top 10 will each have proportion 0.01 -> ROUND to 0.01
    assertTrue(result[0][17].contains(":"), "Full analyze should collect hot_value for int column")

    // Test 3: Full analyze with special characters in values
    sql """drop table if exists full_hot_special"""
    sql """CREATE TABLE full_hot_special (
            key1 int NULL,
            value1 varchar(50) NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """insert into full_hot_special select number, " : ;a" from numbers("number"="100")"""

    sql """analyze table full_hot_special with sync"""
    result = sql """show column stats full_hot_special(value1)"""
    logger.info("Test3 result: " + result)
    assertEquals(1, result.size())
    // All 100 rows have the same value " : ;a", so it should appear with ratio 1.0
    assertEquals("' : ;a':1.0", result[0][17])

    // Test 4: Full analyze then sample analyze, hot_value should be updated
    sql """drop stats full_hot_skew"""
    sql """analyze table full_hot_skew with sample rows 400 with sync"""
    result = sql """show column stats full_hot_skew(value1)"""
    logger.info("Test4 result: " + result)
    assertEquals(1, result.size())
    assertTrue(result[0][17].contains(":"), "Sample analyze should also collect hot_value")
    hotValues = result[0][17].split(";")
    assertEquals(2, hotValues.length)
    assertTrue(hotValues[0].trim() == "'1':0.5" || hotValues[0].trim() == "'0':0.5")
    assertTrue(hotValues[1].trim() == "'1':0.5" || hotValues[1].trim() == "'0':0.5")

    // Test 5: Verify full analyze produces same hot_value as sample analyze for same data
    sql """drop stats full_hot_skew"""
    sql """analyze table full_hot_skew with sync"""
    def fullResult = sql """show column stats full_hot_skew(value1)"""
    logger.info("Test5 full result: " + fullResult)
    assertEquals(1, fullResult.size())
    assertTrue(fullResult[0][17].contains(":"))
    def fullParts = fullResult[0][17].split(";").collect { it.trim() }.sort()

    sql """drop stats full_hot_skew"""
    sql """analyze table full_hot_skew with sample rows 40000 with sync"""
    def sampleResult = sql """show column stats full_hot_skew(value1)"""
    logger.info("Test5 sample result: " + sampleResult)
    assertEquals(1, sampleResult.size())
    assertTrue(sampleResult[0][17].contains(":"))
    // Both full and sample should produce the same hot_value entries (order may differ)
    def sampleParts = sampleResult[0][17].split(";").collect { it.trim() }.sort()
    assertEquals(fullParts, sampleParts)

    // Test 6: Full analyze on empty table should produce an empty hot_value string
    sql """drop table if exists full_hot_empty"""
    sql """CREATE TABLE full_hot_empty (
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
    sql """analyze table full_hot_empty with sync"""
    result = sql """show column stats full_hot_empty(value1)"""
    logger.info("Test6 empty table result: " + result)
    assertEquals(1, result.size())
    assertEquals("0.0", result[0][2])
    assertEquals("''", result[0][17])

    // Test 7: Full analyze on all-NULL column should produce an empty hot_value string
    sql """drop table if exists full_hot_all_null"""
    sql """CREATE TABLE full_hot_all_null (
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
    sql """insert into full_hot_all_null select number, null from numbers("number"="100")"""
    sql """analyze table full_hot_all_null with sync"""
    result = sql """show column stats full_hot_all_null(value1)"""
    logger.info("Test7 all-null result: " + result)
    assertEquals(1, result.size())
    assertEquals("100.0", result[0][2])
    assertEquals("100.0", result[0][4])
    assertEquals("''", result[0][17])

    sql """drop database if exists test_full_analyze_hot_value"""
}
