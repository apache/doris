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
        while(true) {
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
                break;
            }
        }
    }

    def wait_row_count_reported = { ->
        while(true) {
            Thread.sleep(5000)
            boolean reported = true;
            def result = sql """SHOW DATA;"""
            logger.info("result " + result)
            for (int i = 0; i < result.size(); i++) {
                if (result[i][1] == "0.000 ") {
                    reported = false;
                    break;
                }
            }
            if (reported) {
                break;
            }
        }
    }

    def wait_auto_analyze_finish = { table ->
        while(true) {
            Thread.sleep(1000)
            boolean finished = true;
            def result = sql """SHOW AUTO ANALYZE ${table};"""
            logger.info("wait auto analyze finish: " + result)
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
                break;
            }
        }
    }

    sql """drop database if exists test_analyze_mv"""
    sql """create database test_analyze_mv"""
    sql """use test_analyze_mv"""

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
    sql """create materialized view mv1 as select key1 from mvTestDup;"""
    wait_mv_finish("test_analyze_mv", "mvTestDup")
    sql """create materialized view mv2 as select key2 from mvTestDup;"""
    wait_mv_finish("test_analyze_mv", "mvTestDup")
    sql """create materialized view mv3 as select key1, key2, sum(value1), max(value2), min(value3) from mvTestDup group by key1, key2;"""
    wait_mv_finish("test_analyze_mv", "mvTestDup")
    sql """insert into mvTestDup values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestDup with sync;"""

    def result_sample = sql """show column stats mvTestDup"""
    assertEquals(12, result_sample.size())

    result_sample = sql """show column stats mvTestDup(key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestDup(value1)"""
    assertEquals(1, result_sample.size())
    assertEquals("value1", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("3", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

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

    result_sample = sql """show column stats mvTestDup(`mva_SUM__CAST(``value1`` AS BIGINT)`)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_SUM__CAST(`value1` AS BIGINT)", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("6", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestDup(`mva_MAX__``value2```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MAX__`value2`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestDup(`mva_MIN__``value3```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MIN__`value3`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("5", result_sample[0][7])
    assertEquals("5001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])


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

    sql """create materialized view mv1 as select key2 from mvTestAgg;"""
    wait_mv_finish("test_analyze_mv", "mvTestAgg")
    sql """create materialized view mv3 as select key1, key2, sum(value1), max(value2), min(value3) from mvTestAgg group by key1, key2;"""
    wait_mv_finish("test_analyze_mv", "mvTestAgg")
    sql """create materialized view mv6 as select key1, sum(value1) from mvTestAgg group by key1;"""
    wait_mv_finish("test_analyze_mv", "mvTestAgg")
    sql """insert into mvTestAgg values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestAgg with sync;"""
    result_sample = sql """show column stats mvTestAgg"""
    assertEquals(13, result_sample.size())

    result_sample = sql """show column stats mvTestAgg(key2)"""
    assertEquals(1, result_sample.size())
    assertEquals("key2", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("5.0", result_sample[0][3])
    assertEquals("2", result_sample[0][7])
    assertEquals("2001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestAgg(value2)"""
    assertEquals(1, result_sample.size())
    assertEquals("value2", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
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

    sql """create materialized view mv1 as select key1 from mvTestUni;"""
    wait_mv_finish("test_analyze_mv", "mvTestUni")
    sql """create materialized view mv6 as select key2, value2, value3 from mvTestUni;"""
    wait_mv_finish("test_analyze_mv", "mvTestUni")
    sql """insert into mvTestUni values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table mvTestUni with sync;"""

    sql """analyze table mvTestUni with sync;"""
    result_sample = sql """show column stats mvTestUni"""
    assertEquals(9, result_sample.size())

    result_sample = sql """show column stats mvTestUni(key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("5.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("FULL", result_sample[0][9])

    result_sample = sql """show column stats mvTestUni(mv_key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("mv_key1", result_sample[0][0])
    assertEquals("mv1", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
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

    wait_row_count_reported()
    sql """drop stats mvTestDup"""
    result_sample = sql """show column stats mvTestDup"""
    assertEquals(0, result_sample.size())
    sql """analyze database test_analyze_mv PROPERTIES("use.auto.analyzer"="true")"""
    wait_auto_analyze_finish("mvTestDup")

    result_sample = sql """SHOW AUTO ANALYZE mvTestDup;"""
    logger.info("show auto analyze result: " + result_sample)
    def jobId = result_sample[result_sample.size() - 1][0]
    logger.info("Auto analyze job id is " + jobId)

    result_sample = sql """show column stats mvTestDup"""
    assertEquals(12, result_sample.size())

    result_sample = sql """show column stats mvTestDup(key1)"""
    assertEquals(1, result_sample.size())
    assertEquals("key1", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("1", result_sample[0][7])
    assertEquals("1001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(value1)"""
    assertEquals(1, result_sample.size())
    assertEquals("value1", result_sample[0][0])
    assertEquals("N/A", result_sample[0][1])
    assertEquals("6.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("3", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

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
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_SUM__CAST(``value1`` AS BIGINT)`)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_SUM__CAST(`value1` AS BIGINT)", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("6", result_sample[0][7])
    assertEquals("3001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_MAX__``value2```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MAX__`value2`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("4", result_sample[0][7])
    assertEquals("4001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

    result_sample = sql """show column stats mvTestDup(`mva_MIN__``value3```)"""
    assertEquals(1, result_sample.size())
    assertEquals("mva_MIN__`value3`", result_sample[0][0])
    assertEquals("mv3", result_sample[0][1])
    assertEquals("4.0", result_sample[0][2])
    assertEquals("4.0", result_sample[0][3])
    assertEquals("5", result_sample[0][7])
    assertEquals("5001", result_sample[0][8])
    assertEquals("SAMPLE", result_sample[0][9])
    assertEquals("SYSTEM", result_sample[0][11])

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
    verifyTaskStatus(result_sample, "key1", "N/A")
    verifyTaskStatus(result_sample, "key2", "N/A")
    verifyTaskStatus(result_sample, "value1", "N/A")
    verifyTaskStatus(result_sample, "value2", "N/A")
    verifyTaskStatus(result_sample, "value3", "N/A")
    verifyTaskStatus(result_sample, "mv_key1", "mv1")
    verifyTaskStatus(result_sample, "mv_key1", "mv3")
    verifyTaskStatus(result_sample, "mv_key2", "mv2")
    verifyTaskStatus(result_sample, "mv_key2", "mv3")
    verifyTaskStatus(result_sample, "mva_MAX__`value2`", "mv3")
    verifyTaskStatus(result_sample, "mva_MIN__`value3`", "mv3")
    verifyTaskStatus(result_sample, "mva_SUM__CAST(`value1` AS BIGINT)", "mv3")

    sql """drop database if exists test_analyze_mv"""
}

