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

suite("test_show_data_warehouse") {
    def db1Name = "SHOW_DATA_1"
    def db2Name = "SHOW_DATA_2"
    try {
        log.info("db1Name:${db1Name}, db2Name:${db2Name}");
        sql """ DROP DATABASE IF EXISTS ${db1Name}; """
        sql """ DROP DATABASE IF EXISTS ${db2Name}; """
        sql """ CREATE DATABASE ${db1Name}; """
        sql """ CREATE DATABASE ${db2Name}; """
        
        def res = sql """show variables like "%use_v3_storage_format%";""";
        logger.info("session var use_v3_storage_format: ${res}")

        sql """ USE ${db1Name}; """

        sql """ CREATE TABLE `table` (
            `siteid` int(11) NOT NULL COMMENT "",
            `citycode` int(11) NOT NULL COMMENT "",
            `userid` int(11) NOT NULL COMMENT "",
            `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES("replication_num" = "1"); """

        sql """ insert into `table` values
                (9,10,11,12),
                (9,10,11,12),
                (1,2,3,4),
                (13,21,22,16),
                (13,14,15,16),
                (17,18,19,20),
                (1,2,3,4),
                (13,21,22,16),
                (13,14,15,16),
                (17,18,19,20),
                (5,6,7,8),
                (5,6,7,8); """

        sql """ USE ${db2Name}; """

        sql """ CREATE TABLE `table` (
        `siteid` int(11) NOT NULL COMMENT "",
        `citycode` int(11) NOT NULL COMMENT "",
        `userid` int(11) NOT NULL COMMENT "",
        `pv` int(11) NOT NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`siteid`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
        PROPERTIES("replication_num" = "1"); """

        sql """ insert into `table` values
                (9,10,11,12),
                (9,10,11,12),
                (1,2,3,4),
                (13,21,22,16),
                (13,14,15,16); """
    
        // wait for heartbeat

        def showCreateTblRes1 = sql "show create table ${db1Name}.`table`"
        logger.info("db1 table: ${showCreateTblRes1}")
        def showCreateTblRes2 = sql "show create table ${db2Name}.`table`"
        logger.info("db2 table: ${showCreateTblRes2}")

        def res1 = sql """ show data from  ${db1Name}.`table`""";
        def replicaCount1 = res1[0][3].toInteger();
        def res2 = sql """ show data from  ${db2Name}.`table`""";
        def replicaCount2 = res2[0][3].toInteger();
        assertEquals(replicaCount1, replicaCount2);
        log.info("replicaCount: ${replicaCount1}|${replicaCount2}");

        long start = System.currentTimeMillis()
        long dataSize = 0
        long current = -1

        boolean hitDb1 = false;
        boolean hitDb2 = false;

        long db1Size = 957 * replicaCount1
        long db2Size = 908 * replicaCount2
        def result;
        do {
            current = System.currentTimeMillis()
            result = sql """ show data properties("entire_warehouse"="true","db_names"="${db1Name}"); """
            log.info("show data warehouse db1 result: ${result}");
            if ((result.size() == 2) && result[0][1].toInteger() >= db1Size) {
                hitDb1 = true;
            }

            result = sql """ show data properties("entire_warehouse"="true","db_names"="${db2Name}"); """
            log.info("show data warehouse db2 result: ${result}");
            if (result.size() == 2 && result[0][1].toInteger() >= db2Size) {
                hitDb2 = true;
            }
            if (hitDb1 && hitDb2) {
                break
            }
            sleep(5000)
        } while (current - start < 1200 * 1000)

        // because asan be report maybe cost too much time
        assertTrue((hitDb1 && hitDb2), "data size check fail after 1200s")

        result = sql """ show data properties("entire_warehouse"="true","db_names"="${db1Name},${db2Name}"); """
        log.info("show data warehouse db1 and db2 result: ${result}");
        assertEquals(result.size(), 3)
        assert result[0][1].toInteger() >= db1Size
        assert result[1][1].toInteger() >= db2Size
        assert result[2][1].toInteger() >= db1Size + db2Size

        result = sql """show data properties("entire_warehouse"="true")"""
        assertTrue(result.size() >= 3)

        sql """ DROP DATABASE IF EXISTS ${db1Name}; """
        result = sql """show data properties("entire_warehouse"="true")"""
        assertTrue(result.size() > 0)
        for (row : result) {
            if (row[0].toString().equalsIgnoreCase("total")) {
                assertTrue(row[2].toInteger() > 0)
            }
        }
    } finally {
        sql """ DROP DATABASE IF EXISTS ${db1Name} FORCE;"""
        sql """ DROP DATABASE IF EXISTS ${db2Name} FORCE;"""
    }
}
