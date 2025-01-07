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

suite("test_schema_change_statistics") {

    def wait_schema_change_finished = { ->
        Thread.sleep(1000)
        def result = sql """SHOW ALTER TABLE COLUMN from test_schema_change_statistics"""
        boolean finished = false;
        for (int i = 0; i < 120; i++) {
            finished = true;
            for (int j = 0; j < result.size(); j++) {
                if (result[j][9] != "FINISHED") {
                    finished = false;
                    break;
                }
            }
            if (finished) {
                break;
            }
            logger.info("Schema change not finished yet: " + result)
            result = sql """SHOW ALTER TABLE COLUMN from test_schema_change_statistics"""
            Thread.sleep(1000)
        }
    }

    def stats_dropped = { table ->
        def result1 = sql """show column cached stats $table"""
        def result2 = sql """show column stats $table"""
        boolean dropped = false
        for (int i = 0; i < 200; i++) {
            if (0 == result1.size() && 0 == result2.size()) {
                dropped = true;
                break;
            }
            Thread.sleep(1000)
            result1 = sql """show column cached stats $table"""
            result2 = sql """show column stats $table"""
        }
        assertTrue(dropped)
    }

    sql """drop database if exists test_schema_change_statistics"""
    sql """create database test_schema_change_statistics"""
    sql """use test_schema_change_statistics"""
    sql """set global enable_auto_analyze=false"""


    sql """CREATE TABLE change (
        key1 bigint NOT NULL,
        key2 bigint NOT NULL,
        value1 int NOT NULL,
        value2 int NOT NULL,
        value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
    """
    createMV("create materialized view mv1 as select key1,value1,value2 from change;")
    sql """insert into change values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """analyze table change with sync;"""
    def result = sql """show column stats change"""
    assertEquals(8, result.size())
    result = sql """show column cached stats change"""
    assertEquals(8, result.size())

    sql """ALTER TABLE change ADD COLUMN new_value INT DEFAULT "0" AFTER value3;"""
    stats_dropped("change")

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(9, result.size())
    result = sql """show column cached stats change"""
    assertEquals(9, result.size())

    sql """ALTER TABLE change DROP COLUMN new_value;"""
    stats_dropped("change")

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(8, result.size())
    result = sql """show column cached stats change"""
    assertEquals(8, result.size())

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(8, result.size())
    result = sql """show column cached stats change"""
    assertEquals(8, result.size())

    sql """ALTER TABLE change ADD COLUMN new_key INT key DEFAULT "0" AFTER key2;"""
    wait_schema_change_finished()
    stats_dropped("change")

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(9, result.size())
    result = sql """show column cached stats change"""
    assertEquals(9, result.size())

    sql """ALTER TABLE change DROP COLUMN new_key;"""
    wait_schema_change_finished()
    stats_dropped("change")

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(8, result.size())
    result = sql """show column cached stats change"""
    assertEquals(8, result.size())

    sql """analyze table change with sync;"""
    result = sql """show column stats change"""
    assertEquals(8, result.size())
    result = sql """show column cached stats change"""
    assertEquals(8, result.size())


    sql """CREATE TABLE change_no_index (
        key1 bigint NOT NULL,
        key2 bigint NOT NULL,
        value1 int NOT NULL,
        value2 int NOT NULL,
        value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
    """

    sql """insert into change_no_index values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""
    sql """analyze table change_no_index with sync;"""
    result = sql """show column stats change_no_index"""
    assertEquals(5, result.size())
    result = sql """show column cached stats change_no_index"""
    assertEquals(5, result.size())

    sql """ALTER TABLE change_no_index RENAME COLUMN value1 value1_new;"""
    wait_schema_change_finished()
    stats_dropped("change_no_index")


    sql """drop database if exists test_schema_change_statistics"""
}

