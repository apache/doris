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

import org.codehaus.groovy.runtime.IOGroovyMethods
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite ("test_dup_keys_schema_change") {
    def tableName = "schema_change_dup_keys_regression_test"
    def getJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    try {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        sql """ DROP TABLE IF EXISTS ${tableName} """

        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",
                    `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                    `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                    `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                    `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
                DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 8
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "false" );
            """

        sql """ INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 19)
            """

        // alter and test light schema change
        if (!isCloudMode()) {
            sql """ALTER TABLE ${tableName} SET ("light_schema_change" = "true");"""
        }

        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21)
            """

        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20)
            """
        qt_sc """ select count(*) from ${tableName} """

        // add column
        sql """
            ALTER table ${tableName} ADD COLUMN new_column INT default "1" 
            """

        sql """ SELECT * FROM ${tableName} WHERE user_id=2 order by min_dwell_time """

        sql """ INSERT INTO ${tableName} (`user_id`,`date`,`city`,`age`,`sex`,`last_visit_date`,`last_update_date`,
                                        `last_visit_date_not_null`,`cost`,`max_dwell_time`,`min_dwell_time`)
                VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20)
            """

        qt_sc """ SELECT * FROM ${tableName} WHERE user_id=3 """


        sql """ INSERT INTO ${tableName} VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        qt_sc """ SELECT * FROM ${tableName} WHERE user_id = 3 order by new_column """

        qt_sc """ select count(*) from ${tableName} """

        // drop column
        sql """
            ALTER TABLE ${tableName} DROP COLUMN sex
            """
        int max_try_time = 300
        Awaitility.await().atMost(max_try_time, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                return true;
            }
            return false;
        });

        qt_sc """ select * from ${tableName} where user_id = 3 order by new_column """


        sql """ INSERT INTO ${tableName} VALUES
                (4, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """

        qt_sc """ select * from ${tableName} where user_id = 4 """


        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """
        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, 2)
            """

        // compaction
        String[][] tablets = sql """ show tablets from ${tableName}; """
        for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                logger.info("run compaction:" + tablet_id)
                (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                //assertEquals(code, 0)
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
                Awaitility.await().untilAsserted(() -> {
                    String tablet_id = tablet[0]
                    backend_id = tablet[2]
                    (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    return compactionStatus.run_status;
                });
        }

        qt_sc """ select count(*) from ${tableName} """

        qt_sc """  SELECT * FROM ${tableName} WHERE user_id=2 order by min_dwell_time"""

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

}
