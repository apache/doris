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

suite("test_analyze_mv") {

    def wait_mv_finish = { db, table ->
        for (int loop = 0; loop < 300; loop++) {
            Thread.sleep(1000)
            boolean finished = true;
            def result = sql """SHOW ALTER TABLE MATERIALIZED VIEW FROM ${db} WHERE tableName="${table}";"""
            for (int i = 0; i < result.size(); i++) {
                if (result[i][8] != 'FINISHED') {
                    finished = false;
                    break;
                }
            }
            if (finished) {
                return;
            }
        }
        throw new Exception("Wait mv finish timeout.")
    }

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

    def wait_analyze_finish = { table ->
        for (int loop = 0; loop < 300; loop++) {
            Thread.sleep(1000)
            boolean finished = true;
            def result = sql """SHOW ANALYZE ${table};"""
            logger.info("wait analyze finish: " + result)
            if (result.size() <= 0) {
                logger.info("Not analyzed yet.")
                continue;
            }
            for (int i = 0; i < result.size(); i++) {
                if (result[i][9] != 'FINISHED') {
                    finished = false;
                    break;
                }
            }
            if (finished) {
                return;
            }
        }
        throw new Exception("Wait analyze finish timeout.")
    }

    def verify_column_stats = { all_column_result, one_column_result ->
        logger.info("all column result: " + all_column_result)
        logger.info("one column result: " + one_column_result)
        boolean found = false;
        for (int i = 0; i < all_column_result.size(); i++) {
            if (all_column_result[i][0] == one_column_result[0] && all_column_result[i][1] == one_column_result[1]) {
                assertEquals(all_column_result[i][2], one_column_result[2])
                assertEquals(all_column_result[i][3], one_column_result[3])
                assertEquals(all_column_result[i][4], one_column_result[4])
                assertEquals(all_column_result[i][5], one_column_result[5])
                assertEquals(all_column_result[i][6], one_column_result[6])
                assertEquals(all_column_result[i][7], one_column_result[7])
                assertEquals(all_column_result[i][8], one_column_result[8])
                found = true;
            }
        }
        assertTrue(found)
    }

    sql """drop database if exists test_analyze_mv"""
    sql """create database test_analyze_mv"""
    sql """use test_analyze_mv"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    sql """CREATE TABLE mvTestDup (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    def result_row
    if (!isCloudMode()) {
        // Test row count report and report for nereids
        result_row = sql """show index stats mvTestDup mvTestDup"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mvTestDup", result_row[0][1])
        assertEquals("0", result_row[0][3])
        assertEquals("-1", result_row[0][4])
    }

    createMV("create materialized view mv1 as select key1 from mvTestDup;")
    createMV("create materialized view mv2 as select key2 from mvTestDup;")
    createMV("create materialized view mv3 as select key1, key2, sum(value1), max(value2), min(value3) from mvTestDup group by key1, key2;")
    sql """insert into mvTestDup values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestDup with sync;"""

    // Test show index row count
    result_row = sql """show index stats mvTestDup mvTestDup"""
    assertEquals(1, result_row.size())
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mvTestDup", result_row[0][1])
    assertEquals("6", result_row[0][2])
    result_row = sql """show index stats mvTestDup mv1"""
    assertEquals(1, result_row.size())
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mv1", result_row[0][1])
    assertEquals("6", result_row[0][2])
    result_row = sql """show index stats mvTestDup mv2"""
    assertEquals(1, result_row.size())
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mv2", result_row[0][1])
    assertEquals("6", result_row[0][2])
    result_row = sql """show index stats mvTestDup mv3"""
    assertEquals(1, result_row.size())
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mv3", result_row[0][1])
    assertEquals("4", result_row[0][2])

    // Compare show whole table column stats result with show single column.
    def result_all = sql """show column stats mvTestDup"""
    assertEquals(12, result_all.size())
    def result_all_cached = sql """show column cached stats mvTestDup"""
    assertEquals(12, result_all_cached.size())

    def result_sample = sql """show column stats mvTestDup(key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("mvTestDup", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])

    result_sample = sql """show column stats mvTestDup(value1)"""
    assertEquals(1, result_sample.size())
    assertEquals("value1", result_sample[0][0])
    assertEquals("mvTestDup", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("3", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])

    result_sample = sql """show column stats mvTestDup(mv_key1)"""
    assertEquals(2, result_sample.size())
    assertEquals("mv_key1", result_sample[0][0])
    assertTrue(result_sample[0][1] == 'mv1' && result_sample[1][1] == 'mv3' || result_sample[0][1] == 'mv3' && result_sample[1][1] == 'mv1')
    if (result_sample[0][1] == 'mv1') {
        assertEquals("6.0", result_sample[0][2])
    } else {
        assertEquals("4.0", result_sample[0][2])
    }
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])
    verify_column_stats(result_all, result_sample[1])
    verify_column_stats(result_all_cached, result_sample[1])

    result_sample = sql """show column stats mvTestDup(`mva_SUM__CAST(``value1`` AS bigint)`)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_SUM__CAST(`value1` AS bigint)", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("6", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])

    result_sample = sql """show column stats mvTestDup(`mva_MAX__``value2```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MAX__`value2`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])

    result_sample = sql """show column stats mvTestDup(`mva_MIN__``value3```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MIN__`value3`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("5", result_sample[0][7])
    assertEquals("5001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])
    verify_column_stats(result_all, result_sample[0])
    verify_column_stats(result_all_cached, result_sample[0])


    sql """CREATE TABLE mvTestAgg (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int SUM NOT NULL,
            value2 int MAX NOT NULL,
            value3 int MIN NOT NULL
        )ENGINE=OLAP
        AGGREGATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    createMV("create materialized view mv1 as select key2 from mvTestAgg group by key2;")
    createMV("create materialized view mv3 as select key1, key2, sum(value1), max(value2), min(value3) from mvTestAgg group by key1, key2;")
    createMV("create materialized view mv6 as select key1, sum(value1) from mvTestAgg group by key1;")
    sql """alter table mvTestAgg ADD ROLLUP rollup1(key1, value1)"""
    wait_mv_finish("test_analyze_mv", "mvTestAgg")
    sql """insert into mvTestAgg values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestAgg with sync;"""
    result_sample = sql """show column stats mvTestAgg"""
    assertEquals(15, result_sample.size())

    result_sample = sql """show column stats mvTestAgg(key1)"""
    assertEquals(2, result_sample.size())
    if (result_sample[0][1] == "mvTestAgg") {
        assertEquals("key1", result_sample[0][0])
        assertEquals("mvTestAgg", result_sample[0][1])
        assertEquals("5.0", result_sample[0][2])
        assertEquals("4.0", result_sample[0][3])
        assertEquals("1", result_sample[0][7])
        assertEquals("1001", result_sample[0][8])
        assertEquals("key1", result_sample[1][0])
        assertEquals("rollup1", result_sample[1][1])
        assertEquals("4.0", result_sample[1][2])
        assertEquals("4.0", result_sample[1][3])
        assertEquals("1", result_sample[1][7])
        assertEquals("1001", result_sample[1][8])
    } else {
        assertEquals("key1", result_sample[1][0])
        assertEquals("mvTestAgg", result_sample[1][1])
        assertEquals("5.0", result_sample[1][2])
        assertEquals("4.0", result_sample[1][3])
        assertEquals("1", result_sample[1][7])
        assertEquals("1001", result_sample[1][8])
        assertEquals("key1", result_sample[0][0])
        assertEquals("rollup1", result_sample[0][1])
        assertEquals("4.0", result_sample[0][2])
        assertEquals("4.0", result_sample[0][3])
        assertEquals("1", result_sample[0][7])
        assertEquals("1001", result_sample[0][8])
    }

    result_sample = sql """show column stats mvTestAgg(value1)"""
    assertEquals(2, result_sample.size())
    if (result_sample[0][1] == "mvTestAgg") {
        assertEquals("value1", result_sample[0][0])
        assertEquals("mvTestAgg", result_sample[0][1])
        assertEquals("5.0", result_sample[0][2])
        assertEquals("5.0", result_sample[0][3])
        assertEquals("6", result_sample[0][7])
        assertEquals("3001", result_sample[0][8])
        assertEquals("value1", result_sample[1][0])
        assertEquals("rollup1", result_sample[1][1])
        assertEquals("4.0", result_sample[1][2])
        assertEquals("4.0", result_sample[1][3])
        assertEquals("28", result_sample[1][7])
        assertEquals("3001", result_sample[1][8])
    } else {
        assertEquals("value1", result_sample[1][0])
        assertEquals("mvTestAgg", result_sample[1][1])
        assertEquals("5.0", result_sample[1][2])
        assertEquals("5.0", result_sample[1][3])
        assertEquals("6", result_sample[1][7])
        assertEquals("3001", result_sample[1][8])
        assertEquals("value1", result_sample[0][0])
        assertEquals("rollup1", result_sample[0][1])
        assertEquals("4.0", result_sample[0][2])
        assertEquals("4.0", result_sample[0][3])
        assertEquals("28", result_sample[0][7])
        assertEquals("3001", result_sample[0][8])
    }

    result_sample = sql """show column stats mvTestAgg(key2)"""
    assertEquals(1, result_sample.size())
    assertEquals("key2", result_sample[0][0])
    assertEquals("mvTestAgg", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("2", result_sample[0][7])
    assertEquals("2001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestAgg(value2)"""
    assertEquals(1, result_sample.size())
    assertEquals("value2", result_sample[0][0])
    assertEquals("mvTestAgg", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestAgg(mv_key2)"""
    assertEquals(2, result_sample.size())
    assertEquals("mv_key2", result_sample[0][0])
    assertTrue(result_sample[0][1] == 'mv1' && result_sample[1][1] == 'mv3' || result_sample[0][1] == 'mv3' && result_sample[1][1] == 'mv1')
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("2", result_sample[0][7])
    assertEquals("2001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestAgg(`mva_MAX__``value2```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MAX__`value2`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestAgg(`mva_MIN__``value3```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MIN__`value3`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("5", result_sample[0][7])
    assertEquals("5001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])


    sql """
        CREATE TABLE mvTestUni (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "enable_unique_key_merge_on_write" = false,
            "replication_num" = "1"
        );
    """

    createMV("create materialized view mv1 as select key1, key2 from mvTestUni;")
    createMV("create materialized view mv6 as select key1, key2, value2, value3 from mvTestUni;")
    sql """insert into mvTestUni values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestUni with sync;"""
    result_sample = sql """show column stats mvTestUni"""
    assertEquals(11, result_sample.size())

    result_sample = sql """show column stats mvTestUni(key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("mvTestUni", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestUni(mv_key1)"""
    assertEquals(2, result_sample.size())
    assertEquals("mv_key1", result_sample[0][0])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestUni(mv_value2)"""
    assertEquals(1, result_sample.size())
    assertEquals("mv_value2", result_sample[0][0])
    assertEquals("mv6", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("7", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    // Test alter table index row count.
    sql """alter table mvTestDup modify column `value2` set stats ('row_count'='1.5E8', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='1.5E8', 'min_value'='1', 'max_value'='10');"""
    result_row = sql """show index stats mvTestDup mvTestDup;"""
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mvTestDup", result_row[0][1])
    assertEquals("150000000", result_row[0][2])
    sql """alter table mvTestDup index mv1 modify column `mv_key1` set stats ('row_count'='3443', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='1.5E8', 'min_value'='1', 'max_value'='10');"""
    result_row = sql """show index stats mvTestDup mv1;"""
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mv1", result_row[0][1])
    assertEquals("3443", result_row[0][2])
    sql """alter table mvTestDup index mv3 modify column `mva_MAX__``value2``` set stats ('row_count'='234234', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='1.5E8', 'min_value'='1', 'max_value'='10');"""
    result_row = sql """show index stats mvTestDup mv3;"""
    assertEquals("mvTestDup", result_row[0][0])
    assertEquals("mv3", result_row[0][1])
    assertEquals("234234", result_row[0][2])

    sql """drop stats mvTestDup"""
    result_sample = sql """show column stats mvTestDup"""
    assertEquals(0, result_sample.size())

    // Test sample
    try {
        wait_row_count_reported("test_analyze_mv", "mvTestDup", 0, 4, "6")
        wait_row_count_reported("test_analyze_mv", "mvTestDup", 1, 4, "6")
        wait_row_count_reported("test_analyze_mv", "mvTestDup", 2, 4, "4")
        wait_row_count_reported("test_analyze_mv", "mvTestDup", 3, 4, "6")
    } catch (Exception e) {
        logger.info(e.getMessage());
        return;
    }

    if (!isCloudMode()) {
        // Test row count report and report for nereids
        result_row = sql """show index stats mvTestDup mvTestDup"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mvTestDup", result_row[0][1])
        assertEquals("6", result_row[0][3])
        assertEquals("6", result_row[0][4])
        result_row = sql """show index stats mvTestDup mv1"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv1", result_row[0][1])
        assertEquals("6", result_row[0][3])
        assertEquals("6", result_row[0][4])
        result_row = sql """show index stats mvTestDup mv2"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv2", result_row[0][1])
        assertEquals("6", result_row[0][3])
        assertEquals("6", result_row[0][4])
        result_row = sql """show index stats mvTestDup mv3"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv3", result_row[0][1])
        assertEquals("4", result_row[0][3])
        assertEquals("4", result_row[0][4])
    }

    sql """analyze table mvTestDup with sample rows 4000000"""
    wait_analyze_finish("mvTestDup")
    result_sample = sql """SHOW ANALYZE mvTestDup;"""
    logger.info("show analyze result: " + result_sample)
    def jobId = result_sample[result_sample.size() - 1][0]
    logger.info("Analyze job id is " + jobId)

    result_sample = sql """show column stats mvTestDup"""
    assertEquals(12, result_sample.size())

    result_sample = sql """show column stats mvTestDup(key1)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(key1)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("mvTestDup", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(value1)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(value1)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(1, result_sample.size())
    assertEquals("value1", result_sample[0][0])
    assertEquals("mvTestDup", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("3", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(mv_key1)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11] || "MANUAL" != result_sample[1][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(mv_key1)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(2, result_sample.size())
    assertEquals("mv_key1", result_sample[0][0])
    assertTrue(result_sample[0][1] == 'mv1' && result_sample[1][1] == 'mv3' || result_sample[0][1] == 'mv3' && result_sample[1][1] == 'mv1')
    if (result_sample[0][1] == 'mv1') {
        assertEquals("6.0", result_sample[0][2])
    } else {
        assertEquals("4.0", result_sample[0][2])
    }
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_SUM__CAST(``value1`` AS bigint)`)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(`mva_SUM__CAST(``value1`` AS bigint)`)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(1, result_sample.size())
    assertEquals("mva_SUM__CAST(`value1` AS bigint)", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("6", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_MAX__``value2```)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(`mva_MAX__``value2```)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(1, result_sample.size())
    assertEquals("mva_MAX__`value2`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_MIN__``value3```)"""
    logger.info("result " + result_sample)
    if ("MANUAL" != result_sample[0][11]) {
        logger.info("Overwrite by auto analyze, analyze it again.")
        sql """analyze table mvTestDup with sync with sample rows 4000000"""
        result_sample = sql """show column stats mvTestDup(`mva_MIN__``value3```)"""
        logger.info("result after reanalyze " + result_sample)
    }
    assertEquals(1, result_sample.size())
    assertEquals("mva_MIN__`value3`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("5", result_sample[0][7])
    assertEquals("5001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("MANUAL", result_sample[0][11])

    result_sample = sql """show analyze task status ${jobId}"""
    assertEquals(12, result_sample.size())
    def verifyTaskStatus = { result, colName, indexName ->
        def found = false;
        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == colName && result[i][2] == indexName) {
                found = true;
            }
        }
        logger.info("col " + colName + " in index " + indexName + " found ? " + found)
        assertTrue(found)
    }
    verifyTaskStatus(result_sample, "key1", "mvTestDup")
    verifyTaskStatus(result_sample, "key2", "mvTestDup")
    verifyTaskStatus(result_sample, "value1", "mvTestDup")
    verifyTaskStatus(result_sample, "value2", "mvTestDup")
    verifyTaskStatus(result_sample, "value3", "mvTestDup")
    verifyTaskStatus(result_sample, "mv_key1", "mv1")
    verifyTaskStatus(result_sample, "mv_key1", "mv3")
    verifyTaskStatus(result_sample, "mv_key2", "mv2")
    verifyTaskStatus(result_sample, "mv_key2", "mv3")
    verifyTaskStatus(result_sample, "mva_MAX__`value2`", "mv3")
    verifyTaskStatus(result_sample, "mva_MIN__`value3`", "mv3")
    verifyTaskStatus(result_sample, "mva_SUM__CAST(`value1` AS bigint)", "mv3")

    if (!isCloudMode()) {
        // Test row count report and report for nereids
        sql """truncate table mvTestDup"""
        result_row = sql """show index stats mvTestDup mv3"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv3", result_row[0][1])
        assertEquals("0", result_row[0][3])
        assertEquals("-1", result_row[0][4])

        for (int i = 0; i < 120; i++) {
            result_row = sql """show index stats mvTestDup mv3"""
            logger.info("mv3 stats: " + result_row)
            if (result_row[0][4] == "0") {
                break;
            }
            Thread.sleep(5000)
        }
        result_row = sql """show index stats mvTestDup mv3"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv3", result_row[0][1])
        assertEquals("0", result_row[0][3])
        assertEquals("0", result_row[0][4])
        sql """insert into mvTestDup values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""
        result_row = sql """show index stats mvTestDup mv3"""
        assertEquals(1, result_row.size())
        assertEquals("mvTestDup", result_row[0][0])
        assertEquals("mv3", result_row[0][1])
        assertEquals("-1", result_row[0][4])
    }

    // Test alter column stats
    sql """drop stats mvTestDup"""
    sql """alter table mvTestDup modify column key1 set stats ('ndv'='1', 'num_nulls'='1', 'min_value'='10', 'max_value'='40', 'row_count'='50');"""
    sql """alter table mvTestDup index mv3 modify column mv_key1 set stats ('ndv'='5', 'num_nulls'='0', 'min_value'='0', 'max_value'='4', 'row_count'='5');"""
    sql """alter table mvTestDup index mv3 modify column `mva_SUM__CAST(``value1`` AS bigint)` set stats ('ndv'='10', 'num_nulls'='2', 'min_value'='1', 'max_value'='5', 'row_count'='11');"""

    def result = sql """show column cached stats mvTestDup(key1)"""
    assertEquals(1, result.size())
    assertEquals("key1", result[0][0])
    assertEquals("mvTestDup", result[0][1])
    assertEquals("50.0", result[0][2])
    assertEquals("1.0", result[0][3])
    assertEquals("1.0", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("0.0", result[0][6])
    assertEquals("10", result[0][7])
    assertEquals("40", result[0][8])

    result = sql """show column cached stats mvTestDup(mv_key1)"""
    assertEquals(1, result.size())
    assertEquals("mv_key1", result[0][0])
    assertEquals("mv3", result[0][1])
    assertEquals("5.0", result[0][2])
    assertEquals("5.0", result[0][3])
    assertEquals("0.0", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("0.0", result[0][6])
    assertEquals("0", result[0][7])
    assertEquals("4", result[0][8])

    result = sql """show column cached stats mvTestDup(`mva_SUM__CAST(``value1`` AS bigint)`)"""
    assertEquals(1, result.size())
    assertEquals("mva_SUM__CAST(`value1` AS bigint)", result[0][0])
    assertEquals("mv3", result[0][1])
    assertEquals("11.0", result[0][2])
    assertEquals("10.0", result[0][3])
    assertEquals("2.0", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("0.0", result[0][6])
    assertEquals("1", result[0][7])
    assertEquals("5", result[0][8])

    sql """drop database if exists test_analyze_mv"""
}

