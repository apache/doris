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

suite("test_show_partition_stats") {

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

    sql """drop database if exists test_show_partition_stats"""
    sql """create database test_show_partition_stats"""
    sql """use test_show_partition_stats"""
    def enable = sql """show variables like "%enable_partition_analyze%" """
    if (enable[0][1].equalsIgnoreCase("false")) {
        logger.info("partition analyze disabled. " + enable)
        return;
    }

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

    sql """analyze table part with sync;"""
    sql """Insert into part values (1, 1, 1, 1, 1, 1, 1.1, 1.1, 1.1), (2, 2, 2, 2, 2, 2, 2.2, 2.2, 2.2), (3, 3, 3, 3, 3, 3, 3.3, 3.3, 3.3),(4, 4, 4, 4, 4, 4, 4.4, 4.4, 4.4),(5, 5, 5, 5, 5, 5, 5.5, 5.5, 5.5),(6, 6, 6, 6, 6, 6, 6.6, 6.6, 6.6),(10001, 10001, 10001, 10001, 10001, 10001, 10001.10001, 10001.10001, 10001.10001),(10002, 10002, 10002, 10002, 10002, 10002, 10002.10002, 10002.10002, 10002.10002),(10003, 10003, 10003, 10003, 10003, 10003, 10003.10003, 10003.10003, 10003.10003),(10004, 10004, 10004, 10004, 10004, 10004, 10004.10004, 10004.10004, 10004.10004),(10005, 10005, 10005, 10005, 10005, 10005, 10005.10005, 10005.10005, 10005.10005),(10006, 10006, 10006, 10006, 10006, 10006, 10006.10006, 10006.10006, 10006.10006),(20001, 20001, 20001, 20001, 20001, 20001, 20001.20001, 20001.20001, 20001.20001),(20002, 20002, 20002, 20002, 20002, 20002, 20002.20002, 20002.20002, 20002.20002),(20003, 20003, 20003, 20003, 20003, 20003, 20003.20003, 20003.20003, 20003.20003),(20004, 20004, 20004, 20004, 20004, 20004, 20004.20004, 20004.20004, 20004.20004),(20005, 20005, 20005, 20005, 20005, 20005, 20005.20005, 20005.20005, 20005.20005),(20006, 20006, 20006, 20006, 20006, 20006, 20006.20006, 20006.20006, 20006.20006)"""

    def result = sql """show table stats part"""
    assertEquals(1, result.size())
    sql """drop database if exists test_show_partition_stats"""
}

