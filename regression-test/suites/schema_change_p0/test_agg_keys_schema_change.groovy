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

suite ("test_agg_keys_schema_change") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }
    
    def tableName = "schema_change_agg_keys_regression_test"

    try {

        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        sql """ DROP TABLE IF EXISTS schema_change_agg_keys_regression_test """
        sql """
                CREATE TABLE IF NOT EXISTS schema_change_agg_keys_regression_test (
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

        sql """ INSERT INTO schema_change_agg_keys_regression_test VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1))
            """
        sql """ INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2))
            """
        qt_sc """ select * from schema_change_agg_keys_regression_test order by user_id"""

        // alter and test light schema change
        if (!isCloudMode()) {
            sql """ALTER TABLE ${tableName} SET ("light_schema_change" = "true");"""
        }

        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
            """
        sql """ INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(3), to_bitmap(3))
            """

        qt_sc """ select * from schema_change_agg_keys_regression_test order by user_id"""

        // add key column case 1, not light schema change
        sql """
            ALTER table ${tableName} ADD COLUMN new_key_column INT default "2" 
            """

        int max_try_time = 3000
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

        sql """ INSERT INTO ${tableName} (`user_id`,`date`,`city`,`age`,`sex`,`cost`,`max_dwell_time`,`min_dwell_time`, `hll_col`, `bitmap_col`)
                VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, 100, 32, 20, hll_hash(4), to_bitmap(4))
            """
       qt_sc """SELECT * FROM ${tableName} WHERE user_id = 3"""

        // add key column case 2
        sql """ INSERT INTO ${tableName} VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, 3, 110, 32, 20, hll_hash(4), to_bitmap(4))
            """
        qt_sc """ SELECT * FROM ${tableName} WHERE user_id = 3 """


        qt_sc """ select count(*) from ${tableName} """

        // test add double or float key column
        test {
            sql "ALTER table ${tableName} ADD COLUMN new_key_column_double DOUBLE"
            exception "Float or double can not used as a key, use decimal instead."
        }

        test {
            sql "ALTER table ${tableName} ADD COLUMN new_key_column_float FLOAT"
            exception "Float or double can not used as a key, use decimal instead."
        }

        // test modify key column type to double or float
        test {
            sql "ALTER table ${tableName} MODIFY COLUMN age FLOAT"
            exception "Float or double can not used as a key, use decimal instead."
        }

        test {
            sql "ALTER table ${tableName} MODIFY COLUMN age DOUBLE"
            exception "Float or double can not used as a key, use decimal instead."
        }

        // drop key column, not light schema change
        sql """
            ALTER TABLE ${tableName} DROP COLUMN new_key_column
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
        qt_sc """ select * from ${tableName} where user_id = 3 """


        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, 1, 32, 20, hll_hash(5), to_bitmap(5))
            """

        // compaction
        String[][] tablets = sql """ show tablets from ${tableName}; """
        for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                logger.info("run compaction:" + tablet_id)
                (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
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

        qt_sc """  SELECT * FROM schema_change_agg_keys_regression_test WHERE user_id=2 """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
