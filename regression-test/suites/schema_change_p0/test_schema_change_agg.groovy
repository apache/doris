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

suite("test_schema_change_agg", "p0") {
    def tableName3 = "test_all_agg"

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

    sql """ alter table ${tableName3} modify column k2 bigint(11) key NULL"""

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    /*
    sql """ create materialized view view_1 as select k2, k1, k4, k5 from ${tableName3} """
    sleep(10)
    max_try_num = 60
    while (max_try_num--) {
        String res = getCreateViewState(tableName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            execStreamLoad()
            if (max_try_num < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    */

    sql """ alter table ${tableName3} modify column k4 bigint(11) sum NULL"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql """ alter table ${tableName3} add column v14 int sum NOT NULL default "0" after k13 """
    sql """ insert into ${tableName3} values (10001, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 10086) """

    sql """ alter table ${tableName3} modify column v14 int sum NULL default "0" """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql """ alter table ${tableName3} drop column v14 """
    execStreamLoad()

    sql """ alter table ${tableName3} add column v14 int sum NOT NULL default "0" after k13 """

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.81,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', 10086) """

    sql """ alter table ${tableName3} drop column v14 """

    List<List<Object>> result  = sql """ select * from ${tableName3} """
    for (row : result) {
        assertEquals(2, row[1]);
        assertEquals(3, row[2]);
    }

    // boolean type
    sql """ alter table ${tableName3} add column v15 boolean replace NOT NULL default "0" after k13 """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName3}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql """ insert into ${tableName3} values (10002, 2, 3, 4, 5, 6.6, 1.7, 8.81,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00', true) """

    test {
        sql """ALTER table ${tableName3} modify COLUMN v15 int replace NOT NULL default "0" after k13"""
        exception "Can not change BOOLEAN to INT"
    }

    // check add column without agg type
    test {
        sql """ alter table ${tableName3} add column v16 int NOT NULL default "0" after k13 """
        exception "Invalid column order. value should be after key. index[${tableName3}]"
    }

    // del key
    test {
        sql """ alter table ${tableName3} drop column k1 """
        exception "Can not drop key column when table has value column with REPLACE aggregation method"
    }


    //drop partition key
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
        CREATE TABLE `${tableName3}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        PARTITION BY RANGE(`siteid`)
                (
                    partition `old_p1` values [("1"), ("2")),
                    partition `old_p2` values [("2"), ("3"))
                )
        DISTRIBUTED BY HASH(citycode) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    test {
        sql "alter table ${tableName3} drop column siteid"
        exception "Partition column[siteid] cannot be dropped. index[${tableName3}]"
    }

    //modify col

    // without agg type
    test {
        sql "alter table ${tableName3} modify column pv varchar"
        exception "Can not change aggregation type"
    }

    //partition col
    test {
        sql "alter table ${tableName3} modify column siteid varchar DEFAULT '10'"
        exception "Can not modify partition column[siteid]."
    }

    //distribution key

    test {
        sql "alter table ${tableName3} modify column citycode smallint  comment 'citycode'"
        exception "Can not modify distribution column[citycode]. index[${tableName3}]"
    }



}

