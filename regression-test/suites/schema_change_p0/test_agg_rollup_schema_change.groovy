      
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

suite ("test_agg_rollup_schema_change") {
    def tableName = "schema_change_agg_rollup_regression_test"
    def getMVJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1 """
         return jobStateResult[0][8]
    }
    def getJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }
    def waitForMVJob =  (tbName, timeout) -> {
        while (timeout--){
            String result = getMVJobState(tbName)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                sleep(100)
                if (timeout < 1){
                    assertEquals(1,2)
                }
            }
        }
    }

    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
                    `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                    `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
                AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 8
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "false" );
            """

        //add rollup
        def rollupName = "rollup_cost"
        sql "ALTER TABLE ${tableName} ADD ROLLUP ${rollupName}(`user_id`,`date`,`age`, `cost`);"
        waitForMVJob(tableName, 3000)

        sql """ INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1))
            """
        sql """ INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2))
            """
        qt_sc """ select * from ${tableName} order by user_id """

        // alter and test light schema change
        if (!isCloudMode()) {
            sql """ALTER TABLE ${tableName} SET ("light_schema_change" = "true");"""
        }

        //add rollup
        def rollupName2 = "rollup_city"
        sql "ALTER TABLE ${tableName} ADD ROLLUP ${rollupName2}(`user_id`,`city`);"
        waitForMVJob(tableName, 3000)

        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
            """
        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(3), to_bitmap(3))
            """

        qt_sc """ select * from ${tableName} order by user_id """

        test {
            sql "ALTER TABLE ${tableName} DROP COLUMN cost"
            exception "Can not drop column contained by mv, mv=rollup_cost"
        }

        sql""" drop materialized view rollup_cost on ${tableName}; """

        // drop column
        sql """
            ALTER TABLE ${tableName} DROP COLUMN cost
            """

        max_try_time = 3000
        while (max_try_time--){
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }

        sql """ INSERT INTO ${tableName} (`user_id`, `date`, `city`, `age`, `sex`, `max_dwell_time`,`min_dwell_time`, `hll_col`, `bitmap_col`)
                VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(4), to_bitmap(4))
            """
        qt_sc """ SELECT * FROM ${tableName} WHERE user_id = 3 """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 32, 20, hll_hash(5), to_bitmap(5))
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
                boolean running = true
                do {
                    Thread.sleep(100)
                    String tablet_id = tablet[0]
                    backend_id = tablet[2]
                    (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running)
        }
         
        qt_sc """ select count(*) from ${tableName} """

        qt_sc """  SELECT * FROM ${tableName} WHERE user_id=2 """

    } finally {
            //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
