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

suite("test_drop_stats_and_truncate") {

    sql """drop database if exists test_drop_stats_and_truncate"""
    sql """create database test_drop_stats_and_truncate"""
    sql """use test_drop_stats_and_truncate"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE non_part  (
            r_regionkey      int NOT NULL,
            r_name       VARCHAR(25) NOT NULL,
            r_comment    VARCHAR(152)
        )ENGINE=OLAP
        DUPLICATE KEY(`r_regionkey`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """CREATE TABLE `part` (
            `id` INT NULL,
            `colint` INT NULL,
            `coltinyint` tinyint NULL,
            `colsmallint` smallINT NULL,
            `colbigint` bigINT NULL,
            `collargeint` largeINT NULL,
            `colfloat` float NULL,
            `coldouble` double NULL,
            `coldecimal` decimal(27, 9) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES [("-2147483648"), ("10000")),
            PARTITION p2 VALUES [("10000"), ("20000")),
            PARTITION p3 VALUES [("20000"), ("30000"))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql """insert into non_part values (1, "1", "1");"""
    sql """analyze table non_part with sync"""

    def result = sql """show column cached stats non_part"""
    assertEquals(3, result.size())
    result = sql """show column stats non_part"""
    assertEquals(3, result.size())
    result = sql """show table stats non_part"""
    def all_columns = result[0][4]
    String[] columns = all_columns.split(",");
    assertEquals(3, columns.size())

    sql """drop stats non_part(r_comment)"""
    result = sql """show column cached stats non_part"""
    assertEquals(2, result.size())
    result = sql """show column stats non_part"""
    assertEquals(2, result.size())
    result = sql """show table stats non_part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(2, columns.size())

    sql """drop stats non_part"""
    result = sql """show column cached stats non_part"""
    assertEquals(0, result.size())
    result = sql """show column stats non_part"""
    assertEquals(0, result.size())
    result = sql """show table stats non_part"""
    all_columns = result[0][4]
    assertEquals("", all_columns)

    sql """analyze table non_part with sync"""
    result = sql """show column cached stats non_part"""
    assertEquals(3, result.size())
    result = sql """show column stats non_part"""
    assertEquals(3, result.size())
    result = sql """show table stats non_part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(3, columns.size())

    sql """truncate table non_part"""
    result = sql """show column stats non_part"""
    assertEquals(0, result.size())
    result = sql """show table stats non_part"""
    all_columns = result[0][4]
    assertEquals("", all_columns)

    sql """Insert into part values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part with sync"""
    result = sql """show column cached stats part"""
    assertEquals(9, result.size())
    result = sql """show column stats part"""
    assertEquals(9, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(9, columns.size())

    sql """drop stats part(colint)"""
    result = sql """show column cached stats part"""
    assertEquals(8, result.size())
    result = sql """show column stats part"""
    assertEquals(8, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(8, columns.size())

    sql """drop stats part"""
    result = sql """show column cached stats part"""
    assertEquals(0, result.size())
    result = sql """show column stats part"""
    assertEquals(0, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    assertEquals("", all_columns)

    sql """analyze table part with sync"""
    result = sql """show column cached stats part"""
    assertEquals(9, result.size())
    result = sql """show column stats part"""
    assertEquals(9, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(9, columns.size())

    sql """truncate table part"""
    result = sql """show column stats part"""
    assertEquals(0, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    assertEquals("", all_columns)

    sql """Insert into part values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part with sync"""
    result = sql """show column cached stats part"""
    assertEquals(9, result.size())
    result = sql """show column stats part"""
    assertEquals(9, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(9, columns.size())

    sql """truncate table part partition(p1)"""
    result = sql """show column cached stats part"""
    assertEquals(9, result.size())
    result = sql """show column stats part"""
    assertEquals(9, result.size())
    result = sql """show table stats part"""
    all_columns = result[0][4]
    columns = all_columns.split(",");
    assertEquals(9, columns.size())

    sql """drop database if exists test_drop_stats_and_truncate"""
}

