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

suite("test_partition_stats") {


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
        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url) {
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

    def enable = sql """show variables like "%enable_partition_analyze%" """
    if (enable[0][1].equalsIgnoreCase("false")) {
        logger.info("partition analyze disabled. " + enable)
        return;
    }
    sql """set global enable_auto_analyze=false"""
    sql """drop database if exists test_partition_stats"""
    sql """create database test_partition_stats"""
    sql """use test_partition_stats"""
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
    sql """Insert into part values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""

    // Don't record partition update rows until first analyze finish.
    def result = sql """show table stats part partition(*)"""
    assertEquals(0, result.size())
    sql """analyze table part with sync;"""
    result = sql """show table stats part"""
    assertEquals(1, result.size())
    assertEquals("0", result[0][0])

    // Test show cached partition stats.
    sql """analyze table part with sync;"""
    for (int i = 0; i < 20; i++) {
        result = sql """show column cached stats part partition(*)"""
        if (result.size() == 27) {
            logger.info("cache is ready.")
            break;
        }
        logger.info("cache is not ready yet.")
        Thread.sleep(1000)
    }
    result = sql """show column cached stats part(id) partition(p1)"""
    assertEquals("id", result[0][0])
    assertEquals("p1", result[0][1])
    assertEquals("part", result[0][2])
    assertEquals("6.0", result[0][3])
    assertEquals("6", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("1.0", result[0][6])
    assertEquals("6.0", result[0][7])
    assertEquals("24.0", result[0][8])
    assertEquals("N/A", result[0][10])
    assertEquals("N/A", result[0][11])
    result = sql """show column cached stats part partition(p1)"""
    assertEquals(9, result.size())
    result = sql """show column cached stats part partition(*)"""
    assertEquals(27, result.size())

    sql """CREATE TABLE `part1` (
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
    sql """Insert into part1 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part1 with sync;"""

    // Test drop expired stats.
    def dbId
    def tblIdPart
    def tblIdPart1
    result = sql """show catalogs"""

    result = sql """show proc '/catalogs/0'"""
    for (int i = 0; i < result.size(); i++) {
        if (result[i][1] == 'test_partition_stats') {
            dbId = result[i][0]
        }
    }
    result = sql """show proc '/catalogs/0/$dbId'"""
    for (int i = 0; i < result.size(); i++) {
        if (result[i][1] == 'part') {
            tblIdPart = result[i][0]
        }
        if (result[i][1] == 'part1') {
            tblIdPart1 = result[i][0]
        }
    }

    result = sql """select * from internal.__internal_schema.partition_statistics where tbl_id = ${tblIdPart}"""
    assertEquals(27, result.size())
    result = sql """select * from internal.__internal_schema.partition_statistics where tbl_id = ${tblIdPart1}"""
    assertEquals(27, result.size())
    sql """drop table part"""
    sql """drop expired stats"""
    result = sql """select * from internal.__internal_schema.partition_statistics where tbl_id = ${tblIdPart}"""
    assertEquals(0, result.size())
    result = sql """select * from internal.__internal_schema.partition_statistics where tbl_id = ${tblIdPart1}"""
    assertEquals(27, result.size())
    sql """drop database test_partition_stats"""
    sql """drop expired stats"""
    result = sql """select * from internal.__internal_schema.partition_statistics where tbl_id = ${tblIdPart1}"""
    assertEquals(0, result.size())

    // Test analyze table after drop partition
    sql """drop database if exists test_partition_stats"""
    sql """create database test_partition_stats"""
    sql """use test_partition_stats"""
    sql """CREATE TABLE `part2` (
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
    sql """analyze table part2 with sync;"""
    sql """Insert into part2 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part2 with sync;"""
    result = sql """show column stats part2"""
    assertEquals(9, result.size())
    assertEquals("18.0", result[0][2])
    assertEquals("18.0", result[1][2])
    assertEquals("18.0", result[2][2])
    assertEquals("18.0", result[3][2])
    assertEquals("18.0", result[4][2])
    assertEquals("18.0", result[5][2])
    assertEquals("18.0", result[6][2])
    assertEquals("18.0", result[7][2])
    assertEquals("18.0", result[8][2])
    result = sql """show column stats part2 partition(*)"""
    assertEquals(27, result.size())
    sql """alter table part2 drop partition p3"""
    result = sql """show table stats part2"""
    assertEquals("true", result[0][6])
    sql """analyze table part2 with sync;"""
    result = sql """show table stats part2"""
    assertEquals("false", result[0][6])
    result = sql """show column stats part2 partition(*)"""
    assertEquals(18, result.size())
    result = sql """show column stats part2 partition(p3)"""
    assertEquals(0, result.size())
    result = sql """show column stats part2 partition(p1)"""
    assertEquals(9, result.size())
    result = sql """show column stats part2 partition(p2)"""
    assertEquals(9, result.size())
    result = sql """show column stats part2"""
    assertEquals(9, result.size())
    assertEquals("12.0", result[0][2])
    assertEquals("12.0", result[1][2])
    assertEquals("12.0", result[2][2])
    assertEquals("12.0", result[3][2])
    assertEquals("12.0", result[4][2])
    assertEquals("12.0", result[5][2])
    assertEquals("12.0", result[6][2])
    assertEquals("12.0", result[7][2])
    assertEquals("12.0", result[8][2])
    result = sql """show column cached stats part2"""
    assertEquals(9, result.size())
    assertEquals("12.0", result[0][2])
    assertEquals("12.0", result[1][2])
    assertEquals("12.0", result[2][2])
    assertEquals("12.0", result[3][2])
    assertEquals("12.0", result[4][2])
    assertEquals("12.0", result[5][2])
    assertEquals("12.0", result[6][2])
    assertEquals("12.0", result[7][2])
    assertEquals("12.0", result[8][2])

    // Test analyze and drop single partition
    sql """CREATE TABLE `part3` (
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
    sql """Insert into part3 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part3 partition(p1) with sync;"""
    result = sql """show column stats part3 partition(*)"""
    assertEquals(9, result.size())
    assertEquals("p1", result[0][1])
    assertEquals("p1", result[1][1])
    assertEquals("p1", result[2][1])
    assertEquals("p1", result[3][1])
    assertEquals("p1", result[4][1])
    assertEquals("p1", result[5][1])
    assertEquals("p1", result[6][1])
    assertEquals("p1", result[7][1])
    assertEquals("p1", result[8][1])
    result = sql """show column cached stats part3 partition(*)"""
    assertEquals(9, result.size())
    result = sql """show column stats part3"""
    assertEquals(0, result.size())
    sql """analyze table part3 partition(*) with sync;"""
    result = sql """show column stats part3 partition(*)"""
    assertEquals(27, result.size())
    result = sql """show column cached stats part3 partition(*)"""
    assertEquals(27, result.size())
    result = sql """show column stats part3"""
    assertEquals(9, result.size())
    result = sql """show column cached stats part3"""
    assertEquals(9, result.size())
    result = sql """drop stats part3 partition(p1)"""
    result = sql """show column stats part3 partition(*)"""
    assertEquals(18, result.size())
    for (int i = 0; i < 18; i++) {
        assertNotEquals('p1', result[i][1])
    }
    result = sql """show column cached stats part3 partition(*)"""
    assertEquals(18, result.size())
    for (int i = 0; i < 18; i++) {
        assertNotEquals('p1', result[i][1])
    }
    sql """drop stats part3 partition(*)"""
    result = sql """show column stats part3 partition(*)"""
    assertEquals(0, result.size())
    result = sql """show column cached stats part3 partition(*)"""
    assertEquals(0, result.size())
    result = sql """show column stats part3"""
    assertEquals(0, result.size())
    result = sql """show column cached stats part3"""
    assertEquals(0, result.size())

    // Test skip big partitions and fallback to sample analyze (manual analyze doesn't skip)
    sql """CREATE TABLE `part4` (
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
    )"""
    sql """Insert into part4 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    wait_row_count_reported("test_partition_stats", "part4", 0, 4, "24")
    result = sql """show tablets from part4"""
    logger.info("tablets: " + result)
    try {
        // Manual doesn't skip
        sql """set global huge_partition_lower_bound_rows = 10"""
        result = sql """ show variables like \"huge_partition_lower_bound_rows\""""
        logger.info("huge partition bound: " + result)
        sql """analyze table part4 with sync;"""
        result = sql """show column stats part4"""
        logger.info("column result" + result)
        assertEquals(9, result.size())
        assertEquals("24.0", result[0][2])
        assertEquals("24.0", result[1][2])
        assertEquals("24.0", result[2][2])
        assertEquals("24.0", result[3][2])
        assertEquals("24.0", result[4][2])
        assertEquals("24.0", result[5][2])
        assertEquals("24.0", result[6][2])
        assertEquals("24.0", result[7][2])
        assertEquals("24.0", result[8][2])
        assertEquals("FULL", result[0][9])
        assertEquals("FULL", result[1][9])
        assertEquals("FULL", result[2][9])
        assertEquals("FULL", result[3][9])
        assertEquals("FULL", result[4][9])
        assertEquals("FULL", result[5][9])
        assertEquals("FULL", result[6][9])
        assertEquals("FULL", result[7][9])
        assertEquals("FULL", result[8][9])
        result = sql """show column stats part4 partition(*)"""
        logger.info("partition result" + result)
        assertEquals(27, result.size())
        result = sql """show column stats part4 partition(p1)"""
        assertEquals(9, result.size())

        sql """analyze table part4 with sync;"""
        result = sql """show column stats part4 partition(*)"""
        assertEquals(27, result.size())
        result = sql """show column stats part4 partition(p1)"""
        assertEquals(9, result.size())

        // Auto analyze skip large partition.
        sql """drop stats part4"""
        result = sql """show column stats part4 partition(*)"""
        assertEquals(0, result.size())
        result = sql """show column stats part4 partition(p1)"""
        assertEquals(0, result.size())
        sql """analyze table part4 properties("use.auto.analyzer"="true")"""
        result = sql """show auto analyze part4"""
        assertTrue(result.size() > 0)
        def index = result.size() - 1;
        def finished = false;
        for (int i = 0; i < 20; i++) {
            if (result[index][9].equals("FINISHED")) {
                finished = true;
                break;
            }
            Thread.sleep(1000)
        }
        if (finished) {
            result = sql """show column stats part4"""
            logger.info("column result" + result)
            assertEquals(9, result.size())
            assertEquals("24.0", result[0][2])
            assertEquals("24.0", result[1][2])
            assertEquals("24.0", result[2][2])
            assertEquals("24.0", result[3][2])
            assertEquals("24.0", result[4][2])
            assertEquals("24.0", result[5][2])
            assertEquals("24.0", result[6][2])
            assertEquals("24.0", result[7][2])
            assertEquals("24.0", result[8][2])
            assertEquals("SAMPLE", result[0][9])
            assertEquals("SAMPLE", result[1][9])
            assertEquals("SAMPLE", result[2][9])
            assertEquals("SAMPLE", result[3][9])
            assertEquals("SAMPLE", result[4][9])
            assertEquals("SAMPLE", result[5][9])
            assertEquals("SAMPLE", result[6][9])
            assertEquals("SAMPLE", result[7][9])
            assertEquals("SAMPLE", result[8][9])
            result = sql """show column stats part4 partition(p1)"""
            assertEquals(0, result.size())
            result = sql """show column stats part4 partition(p2)"""
            assertEquals(9, result.size())
            result = sql """show column stats part4 partition(p3)"""
            assertEquals(9, result.size())
        } else {
            logger.info("Auto analyze is too slow. Stop and return.")
            return
        }
        assertTrue(result.size() > 0)

    } finally {
        sql """set global huge_partition_lower_bound_rows = 100000000"""
    }

    // Test update partition cache while analyzing
    sql """CREATE TABLE `part5` (
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
    )"""
    sql """Insert into part5 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part5 with sync"""
    result = sql """show column cached stats part5 partition(*)"""
    assertEquals(27, result.size())
    result = sql """show column cached stats part5(id) partition(*)"""
    assertEquals(3, result.size())
    result = sql """show column cached stats part5(id) partition(p1)"""
    assertEquals(1, result.size())
    assertEquals("id", result[0][0])
    assertEquals("p1", result[0][1])
    assertEquals("part5", result[0][2])
    assertEquals("12.0", result[0][3])
    assertEquals("6", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("1.0", result[0][6])
    assertEquals("6.0", result[0][7])
    assertEquals("48.0", result[0][8])
    sql """drop stats part5 partition(p1)"""
    result = sql """show column cached stats part5(id) partition(p1)"""
    assertEquals(0, result.size())
    result = sql """show column cached stats part5(id) partition(*)"""
    assertEquals(2, result.size())

    // Test skip partition that doesn't change since last analyze
    sql """CREATE TABLE `part6` (
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
    )"""
    sql """Insert into part6 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""
    sql """analyze table part6 with sync"""
    result = sql """show column stats part6 (id)"""
    def table_time = result[0][13]
    result = sql """show column stats part6 (id) partition(p1)"""
    def p0_time = result[0][9]
    result = sql """show column stats part6 (id) partition(p2)"""
    def p1_time = result[0][9]
    result = sql """show column stats part6 (id) partition(p3)"""
    def p2_time = result[0][9]
    Thread.sleep(1000)
    sql """Insert into part6 values (7, 7, 7, 7, 7, 7, 7.7, 7.7, 7.7)"""
    sql """analyze table part6 with sync"""

    result = sql """show column stats part6 (id)"""
    assertNotEquals(table_time, result[0][13])
    result = sql """show column stats part6 (id) partition(p1)"""
    assertNotEquals(p0_time, result[0][9])
    result = sql """show column stats part6 (id) partition(p2)"""
    assertEquals(p1_time, result[0][9])
    result = sql """show column stats part6 (id) partition(p3)"""
    assertEquals(p2_time, result[0][9])


    // Test auto analyze. Show table stats partition etc..
    sql """CREATE TABLE `part7` (
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
    )"""
    // 1. Test show table stats partition()
    sql """analyze table part7 with sync"""
    sql """Insert into part7 values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004)"""
    result = sql """show table stats part7 partition(*)"""
    assertEquals(3, result.size())
    result = sql """show table stats part7 partition(p1)"""
    assertEquals(1, result.size())
    assertEquals("p1", result[0][0])
    assertEquals("6", result[0][1])
    result = sql """show table stats part7 partition(p2)"""
    assertEquals(1, result.size())
    assertEquals("p2", result[0][0])
    assertEquals("5", result[0][1])
    result = sql """show table stats part7 partition(p3)"""
    assertEquals(1, result.size())
    assertEquals("p3", result[0][0])
    assertEquals("4", result[0][1])

    // 2. Test auto analyze partition table use full analyze.
    wait_row_count_reported("test_partition_stats", "part7", 0, 4, "15")
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(1, result.size())
    def finished = false;
    for (int i = 0; i < 20; i++) {
        if (result[0][9].equals("FINISHED")) {
            finished = true;
            break;
        }
        Thread.sleep(1000)
    }
    if (finished) {
        result = sql """show column stats part7"""
        assertEquals(9, result.size())
        assertEquals("FULL", result[0][9])
        assertEquals("FULL", result[1][9])
        assertEquals("FULL", result[2][9])
        assertEquals("FULL", result[3][9])
        assertEquals("FULL", result[4][9])
        assertEquals("FULL", result[5][9])
        assertEquals("FULL", result[6][9])
        assertEquals("FULL", result[7][9])
        assertEquals("FULL", result[8][9])
    } else {
        logger.info("Auto analyze is too slow. Stop and return.")
        return
    }

    // 3. Test health
    // a. Nothing change. Not analyze.
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(1, result.size())

    // b. One partition changed, but not more than threshold. Not analyze
    sql """Insert into part7 values (7, 7, 7, 7, 7, 7, 7.7, 7.7, 7.7)"""
    result = sql """show table stats part7 partition(p1)"""
    assertEquals(1, result.size())
    assertEquals("p1", result[0][0])
    assertEquals("7", result[0][1])
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(1, result.size())

    // c. Two partitions changed, but not more than threshold. Not analyze
    sql """Insert into part7 values (10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006)"""
    result = sql """show table stats part7 partition(p2)"""
    assertEquals(1, result.size())
    assertEquals("p2", result[0][0])
    assertEquals("6", result[0][1])
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(1, result.size())

    // d. 3 partitions changed, but not more than threshold. Need to analyze
    sql """Insert into part7 values (20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005)"""
    result = sql """show table stats part7 partition(p3)"""
    assertEquals(1, result.size())
    assertEquals("p3", result[0][0])
    assertEquals("5", result[0][1])
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(2, result.size())

    // e. One partition change more than 40% of the data. Need to analyze.
    sql """insert into part7 values (20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005)"""
    result = sql """show table stats part7 partition(p3)"""
    assertEquals(1, result.size())
    assertEquals("p3", result[0][0])
    assertEquals("10", result[0][1])
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(3, result.size())

    // f. Add a partition. Need to analyze
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(3, result.size())
    sql """alter table part7 add partition p4 VALUES [("30000"), ("40000"))"""
    sql """insert into part7 values (30001, 30001, 30001, 30001, 30001, 30001, 30001.30001, 30001.30001, 30001.30001)"""
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(4, result.size())
    result = sql """show column stats part7"""
    assertEquals(9, result.size())
    assertEquals("24.0", result[0][2])
    assertEquals("24.0", result[1][2])
    assertEquals("24.0", result[2][2])
    assertEquals("24.0", result[3][2])
    assertEquals("24.0", result[4][2])
    assertEquals("24.0", result[5][2])
    assertEquals("24.0", result[6][2])
    assertEquals("24.0", result[7][2])
    assertEquals("24.0", result[8][2])
    assertEquals("19.0", result[0][3])
    assertEquals("19.0", result[1][3])
    assertEquals("19.0", result[2][3])
    assertEquals("19.0", result[3][3])
    assertEquals("19.0", result[4][3])
    assertEquals("19.0", result[5][3])
    assertEquals("19.0", result[6][3])
    assertEquals("19.0", result[7][3])
    assertEquals("19.0", result[8][3])
    result = sql """show column stats part7 partition(*)"""
    assertEquals(36, result.size())

    // e. Drop a partition. Need to analyze
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(4, result.size())
    sql """alter table part7 drop partition p4"""
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show auto analyze part7"""
    assertEquals(5, result.size())
    result = sql """show column stats part7"""
    assertEquals(9, result.size())
    assertEquals("23.0", result[0][2])
    assertEquals("23.0", result[1][2])
    assertEquals("23.0", result[2][2])
    assertEquals("23.0", result[3][2])
    assertEquals("23.0", result[4][2])
    assertEquals("23.0", result[5][2])
    assertEquals("23.0", result[6][2])
    assertEquals("23.0", result[7][2])
    assertEquals("23.0", result[8][2])
    assertEquals("18.0", result[0][3])
    assertEquals("18.0", result[1][3])
    assertEquals("18.0", result[2][3])
    assertEquals("18.0", result[3][3])
    assertEquals("18.0", result[4][3])
    assertEquals("18.0", result[5][3])
    assertEquals("18.0", result[6][3])
    assertEquals("18.0", result[7][3])
    assertEquals("18.0", result[8][3])
    result = sql """show column stats part7 partition(*)"""
    assertEquals(27, result.size())

    // Test truncate table
    sql """truncate table part7 partition(p1)"""
    result = sql """show table stats part7 partition(*)"""
    assertEquals(2, result.size())
    assertNotEquals('p1', result[0][0])
    assertNotEquals('p1', result[1][0])
    result = sql """show column stats part7 partition(p1)"""
    assertEquals(0, result.size())
    result = sql """show column cached stats part7 partition(p1)"""
    assertEquals(0, result.size())
    result = sql """show column stats part7 partition(p2)"""
    assertEquals(9, result.size())
    result = sql """show column stats part7 partition(p3)"""
    assertEquals(9, result.size())

    sql """Insert into part7 values (7, 7, 7, 7, 7, 7, 7.7, 7.7, 7.7)"""
    result = sql """show table stats part7 partition(p1)"""
    assertEquals(1, result.size())
    assertEquals("p1", result[0][0])
    assertEquals("1", result[0][1])
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show column stats part7"""
    assertEquals(9, result.size())
    assertEquals("17.0", result[0][2])
    assertEquals("17.0", result[1][2])
    assertEquals("17.0", result[2][2])
    assertEquals("17.0", result[3][2])
    assertEquals("17.0", result[4][2])
    assertEquals("17.0", result[5][2])
    assertEquals("17.0", result[6][2])
    assertEquals("17.0", result[7][2])
    assertEquals("17.0", result[8][2])
    result = sql """show column cached stats part7"""
    assertEquals(9, result.size())
    assertEquals("17.0", result[0][2])
    assertEquals("17.0", result[1][2])
    assertEquals("17.0", result[2][2])
    assertEquals("17.0", result[3][2])
    assertEquals("17.0", result[4][2])
    assertEquals("17.0", result[5][2])
    assertEquals("17.0", result[6][2])
    assertEquals("17.0", result[7][2])
    assertEquals("17.0", result[8][2])
    result = sql """show column stats part7 partition(p1)"""
    assertEquals(9, result.size())
    result = sql """show column cached stats part7 partition(p1)"""
    assertEquals(9, result.size())

    sql """truncate table part7"""
    result = sql """show table stats part7 partition(*)"""
    assertEquals(0, result.size())
    result = sql """show column stats part7"""
    assertEquals(0, result.size())
    result = sql """show column cached stats part7"""
    assertEquals(0, result.size())
    sql """analyze table part7 properties("use.auto.analyzer"="true")"""
    result = sql """show column stats part7"""
    assertEquals(9, result.size())
    assertEquals("0.0", result[0][2])
    assertEquals("0.0", result[1][2])
    assertEquals("0.0", result[2][2])
    assertEquals("0.0", result[3][2])
    assertEquals("0.0", result[4][2])
    assertEquals("0.0", result[5][2])
    assertEquals("0.0", result[6][2])
    assertEquals("0.0", result[7][2])
    assertEquals("0.0", result[8][2])
    result = sql """show column cached stats part7"""
    assertEquals(9, result.size())
    assertEquals("0.0", result[0][2])
    assertEquals("0.0", result[1][2])
    assertEquals("0.0", result[2][2])
    assertEquals("0.0", result[3][2])
    assertEquals("0.0", result[4][2])
    assertEquals("0.0", result[5][2])
    assertEquals("0.0", result[6][2])
    assertEquals("0.0", result[7][2])
    assertEquals("0.0", result[8][2])

    sql """drop database test_partition_stats"""
}

