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

suite ("test_rename_rollup") {
    def tableName = "rename_rollup_test"
    def getRollupJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1 """
         return jobStateResult[0][8]
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
            PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );
        """
    qt_desc """ desc ${tableName} """

    def resRoll = "null"
    def rollupName = "rollup_cost"
    sql "ALTER TABLE ${tableName} ADD ROLLUP ${rollupName}(`user_id`, `cost`);"
    int max_try_time = 3000
    while (max_try_time--){
        String result = getRollupJobState(tableName)
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

    qt_select """ select user_id, cost from ${tableName} order by user_id """

    sql """ INSERT INTO ${tableName} VALUES
            (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1))
        """
    sql """ INSERT INTO ${tableName} VALUES
            (1, '2017-10-02', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2))
        """
    sql """ sync """

    qt_select """ select * from ${tableName} order by user_id """

    qt_select """ select user_id, sum(cost) from ${tableName} group by user_id order by user_id """

    sql """ ALTER TABLE ${tableName} RENAME ROLLUP ${rollupName} new_${rollupName}"""

    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-02', 'Beijing', 10, 1, 1, 32, 20, hll_hash(3), to_bitmap(3))
        """
    qt_desc """ desc ${tableName} ALL"""

    qt_select""" select * from ${tableName} order by user_id """

    qt_select """ select user_id, sum(cost) from ${tableName} group by user_id order by user_id """

    sql """ DROP TABLE ${tableName} """
}

