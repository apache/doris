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

import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_add_keys_light_schema_change") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def getJobTxnId = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][8]
    }

    def getCreateViewState = { tableName ->
        def createViewStateResult = sql """ SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return createViewStateResult[0][8]
    }

    def waitJobFinish = { strSql, statePos -> 
        Object jobStateResult = null
        int max_try_time = 2000
        while (max_try_time--){
            jobStateResult = sql """${strSql}"""
            def jsonRes = JsonOutput.toJson(jobStateResult)
            String result = jobStateResult[0][statePos]
            if (result == "FINISHED") {
                log.info(jsonRes)
                sleep(500)
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }
        return jobStateResult
    }
    
    def tableName = "add_keys_light_schema_change"
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
        int max_try_time = 3000
        List<List<Object>> table_tablets = null;
        List<List<Object>> rowset_cnt = null;

        // case 1.1: light schema change type check: add key column in short key column num
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                    `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                    `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
                AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 20, hll_hash(1), to_bitmap(1))
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER user_id PROPERTIES ("timeout"="604800")
            """
        
        def jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertNotEquals(jobStateResult[0][8], "-1")

        qt_sc """ select * from add_keys_light_schema_change order by user_id """

        // case 1.2: light schema change type check: add key column in mv index
        sql """
            CREATE MATERIALIZED VIEW first_view AS 
            SELECT user_id, date, age, sum(cost) 
            FROM add_keys_light_schema_change 
            GROUP BY user_id, date, age
            """
        waitJobFinish(""" SHOW ALTER TABLE MATERIALIZED VIEW WHERE IndexName = "add_keys_light_schema_change" ORDER BY CreateTime DESC LIMIT 1 """, 8)

        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_mv_key1 INT default "2" TO first_view
            """

        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertNotEquals(jobStateResult[0][8], "-1")
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='first_view' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertNotEquals(jobStateResult[0][8], "-1")

        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_mv_key2 INT default "2"
            """

        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")

        // case 1.3: light schema change type check: add key column with enable_unique_key_merge_on_write
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT DEFAULT "0" COMMENT "用户总消费")
                UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( 
                    "replication_num" = "1", 
                    "light_schema_change" = "true", 
                    "enable_unique_key_merge_on_write" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 20)
            """
        qt_sc """ select * from add_keys_light_schema_change order by user_id """

        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_mv_key1 INT KEY default "2"
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertNotEquals(jobStateResult[0][8], "-1")

        // case 2.1: light schema change aggreage : with multi version rowset and compaction
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                    `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                    `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
                AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 20, hll_hash(1), to_bitmap(1))
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER sex PROPERTIES ("timeout"="604800");
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, hll_hash(2), to_bitmap(2))
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,new_key_column,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 1, 40, hll_hash(3), to_bitmap(3))
            """
        table_tablets = sql """ SHOW TABLETS FROM add_keys_light_schema_change ORDER BY RowCount DESC LIMIT 1 """
        rowset_cnt = sql """ SELECT count(*) FROM information_schema.rowsets WHERE TABLET_ID = ${table_tablets[0][0]} """
        log.info(JsonOutput.toJson(rowset_cnt))
        assertEquals(rowset_cnt[0][0], 4)

        qt_21_agg_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_21_agg_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_21_agg_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_21_agg_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_21_agg_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_21_agg_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        // case 2.2: light schema change aggreage : with drop
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                    `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                    `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
                AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 20, hll_hash(1), to_bitmap(1))
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER sex PROPERTIES ("timeout"="604800");
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, hll_hash(2), to_bitmap(2))
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,new_key_column,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 1, 40, hll_hash(3), to_bitmap(3))
            """
        sql """
                ALTER TABLE add_keys_light_schema_change DROP COLUMN sex;
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertNotEquals(jobStateResult[0][8], "-1")

        qt_22_agg_drop_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_22_agg_drop_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_22_agg_drop_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """
    
        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_22_agg_drop_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_22_agg_drop_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_22_agg_drop_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        // case 2.3: light schema change aggreage : with rollup and multi version rowset and compaction
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `date` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",

                    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                    `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                    `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
                AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 20, hll_hash(1), to_bitmap(1))
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD ROLLUP first_idx (user_id, date, city, age, cost);
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE ROLLUP WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 8)
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER age TO first_idx PROPERTIES ("timeout"="604800");
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='first_idx' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, hll_hash(2), to_bitmap(2))
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,date,city,age,sex,new_key_column,cost,max_dwell_time,hll_col,bitmap_col) VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, 1, 1, 40, hll_hash(3), to_bitmap(3))
            """
        table_tablets = sql """ SHOW TABLETS FROM add_keys_light_schema_change WHERE IndexName = "add_keys_light_schema_change" ORDER BY RowCount DESC LIMIT 1 """
        rowset_cnt = sql """ SELECT count(*) FROM information_schema.rowsets WHERE TABLET_ID = ${table_tablets[0][0]} """
        log.info(JsonOutput.toJson(rowset_cnt))
        assertEquals(rowset_cnt[0][0], 4)

        qt_23_base_table_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_23_base_table_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_23_base_table_multi_rowset """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_23_base_table_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_23_base_table_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_23_base_table_compaction """ SELECT user_id,sum(cost),max(max_dwell_time),hll_union_agg(hll_col),bitmap_union_count(bitmap_col) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """


        table_tablets = sql """ SHOW TABLETS FROM add_keys_light_schema_change WHERE IndexName = "first_idx" ORDER BY RowCount DESC LIMIT 1 """
        rowset_cnt = sql """ SELECT count(*) FROM information_schema.rowsets WHERE TABLET_ID = ${table_tablets[0][0]} """
        log.info(JsonOutput.toJson(rowset_cnt))

        assertEquals(rowset_cnt[0][0], 4)
        qt_23_rollup_multi_rowset """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_23_rollup_multi_rowset """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_23_rollup_multi_rowset """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_23_agg_rollup_compaction """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change GROUP BY user_id; """
        qt_23_agg_rollup_compaction """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change WHERE new_key_column = 2 GROUP BY user_id; """
        qt_23_agg_rollup_compaction """ SELECT user_id,sum(cost) FROM add_keys_light_schema_change WHERE new_key_column = 1 GROUP BY user_id; """

        // case 3.1: light schema change duplicate : with multi version rowset and compaction
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
                    `type` INT NOT NULL COMMENT "日志类型",
                    `error_code` INT COMMENT "错误码",
                    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
                    `op_id` BIGINT COMMENT "负责人id",
                    `op_time` DATETIME COMMENT "处理时间")
                DUPLICATE KEY(`timestamp`, `type`, `error_code`) DISTRIBUTED BY HASH(`type`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                ('2017-10-01 10:00:00', '1', 0, '', 10, '2017-10-01 12:00:00')
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER error_code PROPERTIES ("timeout"="604800");
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        sql """ 
                INSERT INTO add_keys_light_schema_change (timestamp,type,error_code,error_msg,op_id,op_time) VALUES
                ('2017-10-01 10:00:00', '1', 0, 'none', 10, '2017-10-01 12:00:00')
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change (timestamp,type,error_code,new_key_column,error_msg,op_id,op_time) VALUES
                ('2017-10-01 10:00:00', '1', 0, 1, 'none', 10, '2017-10-01 12:00:00')
            """
        table_tablets = sql """ SHOW TABLETS FROM add_keys_light_schema_change ORDER BY RowCount DESC LIMIT 1 """
        rowset_cnt = sql """ SELECT count(*) FROM information_schema.rowsets WHERE TABLET_ID = ${table_tablets[0][0]} """
        log.info(JsonOutput.toJson(rowset_cnt))
        assertEquals(rowset_cnt[0][0], 4)

        qt_31_duplicate_multi_rowset """ SELECT * FROM add_keys_light_schema_change; """
        qt_31_duplicate_multi_rowset """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 2; """
        qt_31_duplicate_multi_rowset """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 1; """

        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_31_duplicate_compaction """ SELECT * FROM add_keys_light_schema_change; """
        qt_31_duplicate_compaction """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 2; """
        qt_31_duplicate_compaction """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 1; """

        // case 4.1: light schema change unique : with multi version rowset and compaction
        sql """ DROP TABLE IF EXISTS add_keys_light_schema_change """
        sql """
                CREATE TABLE IF NOT EXISTS add_keys_light_schema_change (
                    `user_id` LARGEINT NOT NULL COMMENT "用户id",
                    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
                    `city` VARCHAR(20) COMMENT "用户所在城市",
                    `age` SMALLINT COMMENT "用户年龄",
                    `sex` TINYINT COMMENT "用户性别",
                    `phone` LARGEINT COMMENT "用户电话",
                    `address` VARCHAR(500) COMMENT "用户地址",
                    `register_time` DATETIME COMMENT "用户注册时间")
                UNIQUE KEY(`user_id`, `username`, `city`)
                DISTRIBUTED BY HASH(`user_id`)
                BUCKETS 4
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change VALUES
                (1, 'Jone', 'Beijing', 10, 1, 10010, 'Haidian', '2017-10-01 12:00:00')
            """
        sql """
            ALTER TABLE add_keys_light_schema_change ADD COLUMN new_key_column INT default "2" AFTER city PROPERTIES ("timeout"="604800");
            """
        jobStateResult = waitJobFinish(""" SHOW ALTER TABLE COLUMN WHERE IndexName='add_keys_light_schema_change' ORDER BY createtime DESC LIMIT 1 """, 9)
        assertEquals(jobStateResult[0][8], "-1")
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,username,city,age,sex,phone,address,register_time) VALUES
                (1, 'Jone', 'Beijing', 10, 1, 10010, 'Haidian', '2017-10-01 13:00:00')
            """
        sql """ 
                INSERT INTO add_keys_light_schema_change (user_id,username,city,new_key_column,age,sex,phone,address,register_time) VALUES
                (1, 'Jone', 'Beijing', 1, 10, 1, 10010, 'Haidian', '2017-10-01 14:00:00')
            """
        table_tablets = sql """ SHOW TABLETS FROM add_keys_light_schema_change ORDER BY RowCount DESC LIMIT 1 """
        rowset_cnt = sql """ SELECT count(*) FROM information_schema.rowsets WHERE TABLET_ID = ${table_tablets[0][0]} """
        log.info(JsonOutput.toJson(rowset_cnt))
        assertEquals(rowset_cnt[0][0], 4)

        qt_41_unique_multi_rowset """ SELECT * FROM add_keys_light_schema_change; """
        qt_41_unique_multi_rowset """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 2; """
        qt_41_unique_multi_rowset """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 1; """

        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_tablets[0][0])
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

        qt_41_unique_compaction """ SELECT * FROM add_keys_light_schema_change; """
        qt_41_unique_compaction """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 2; """
        qt_41_unique_compaction """ SELECT * FROM add_keys_light_schema_change WHERE new_key_column = 1; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
