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

suite("test_schema_change_agg_check_all_types", "p0") {
    def tableName3 = "test_schema_change_agg_check_all_types"

    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    def getCreateViewState = { tableName ->
        def createViewStateResult = sql """ SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return createViewStateResult[0][8]
    }

    def execStreamLoad = {
        streamLoad {
            table "${tableName3}"

            set 'column_separator', ','

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` int(30) sum NULL,
      `k5` largeint(40) sum NULL,
      `k6` float sum NULL,
      `k7` double sum NULL,
      `k8` decimal(9, 0) max NULL,
      `k9` char(10) replace NULL,
      `k10` varchar(1024) replace NULL,
      `k11` text replace NULL,
      `k12` date replace NULL,
      `k13` datetime replace NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    execStreamLoad()

    // tinyint to smallint
    sql """ alter table ${tableName3} modify column k2 smallint key NULL"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10001, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_tinyint_to_smallint """ select * from ${tableName3} """


    // smallint to int
    sql """ alter table ${tableName3} modify column k2 int key NULL"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_smallint_to_int """ select * from ${tableName3} """

    // int to bigint
    sql """ alter table ${tableName3} modify column k2 bigint key NULL"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10003, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_int_to_bigint """ select * from ${tableName3} """

    // bigint to largeint

    sql """ alter table ${tableName3} modify column k2 largeint key NULL"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10004, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """

    qt_bigint_to_largeint """ select * from ${tableName3} """

    // largeint to float
    sql """ alter table ${tableName3} add column k14 largeint replace not null default "0" after k13"""

    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ alter table ${tableName3} modify column k14 float replace not null default "0" after k13"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10005, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 1.11) """

    qt_largeint_to_float """ select * from ${tableName3} """

    // float to double
    sql """ alter table ${tableName3} modify column k14 double replace not null default "0" after k13"""
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getJobState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_num < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ insert into ${tableName3} values (10006, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00',1.11) """

    qt_float_to_double """ select * from ${tableName3} """

}

