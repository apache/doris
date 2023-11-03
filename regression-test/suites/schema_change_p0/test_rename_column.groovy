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

suite ("test_rename_column") {
    def tableName = "rename_column_test"
    def getMVJobState = { tbName ->
         def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1 """
         return jobStateResult[0][8]
    }
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
            `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
            `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
            `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
            `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
        UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 8
        PROPERTIES ( "replication_num" = "1" , "light_schema_change" = "false")
        """
    qt_desc """ desc ${tableName} """

    sql """ 
        INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20)
        """
    sql """
        INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21)
        """
    sql """ sync """

    // alter and test light schema change
    sql """ALTER TABLE ${tableName} SET ("light_schema_change" = "true");"""

    qt_select """ SELECT * FROM ${tableName} order by user_id ASC, last_visit_date """

    // rename key column
    sql """ ALTER table ${tableName} RENAME COLUMN  user_id new_user_id """

    sql """ sync """

    qt_select """ SELECT * FROM ${tableName} order by new_user_id DESC, last_visit_date """

    qt_desc """ desc ${tableName} """

    sql """
        INSERT INTO ${tableName} VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 32, 22)
        """
    qt_select """ SELECT * FROM ${tableName} order by new_user_id DESC, last_visit_date """

    // rename value column
    sql """
        ALTER table ${tableName} RENAME COLUMN  max_dwell_time new_max_dwell_time
        """
    sql """
        INSERT INTO ${tableName} VALUES
                (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 33, 23)
        """
    sql """ sync """

    qt_select """ SELECT * FROM ${tableName} order by new_user_id DESC, last_visit_date """

    qt_desc """ desc ${tableName} """

    test {
        sql """ ALTER table ${tableName} RENAME COLUMN  date city """
        exception "Column name[city] is already used"
    }

    test {
        sql """ ALTER table ${tableName} RENAME COLUMN  aaa  bbb """
        exception "Column[aaa] does not exists"
    }

    test {
        sql """ ALTER table ${tableName} RENAME COLUMN  date  date """
        exception "Same column name"
    }

    sql """ DROP TABLE ${tableName} """

    // table without column unique id
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
        UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 8
        PROPERTIES ( "replication_num" = "1" , "light_schema_change" = "false")
        """
    test {
        sql """ ALTER table ${tableName} RENAME COLUMN  date new_date """
        exception "not implemented"
    }
    sql """ DROP TABLE ${tableName} """

    // rollup
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

    //add rollup
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

    sql """ ALTER TABLE ${tableName} RENAME COLUMN user_id new_user_id """

    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-02', 'Beijing', 10, 1, 1, 32, 20, hll_hash(3), to_bitmap(3))
        """
    qt_desc """ desc ${tableName} """

    qt_select""" select * from ${tableName} order by new_user_id """

    qt_select """ select new_user_id, sum(cost) from ${tableName} group by new_user_id order by new_user_id """

    sql """ DROP TABLE ${tableName} """

    // materialized view
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

    //add materialized view
    def resMv = "null"
    def mvName = "mv1"
    sql "create materialized view ${mvName} as select user_id, sum(cost) from ${tableName} group by user_id;"
    max_try_time = 3000
    while (max_try_time--){
        String result = getMVJobState(tableName)
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
    String viewName = "renameColumnView1"
    sql "create view ${viewName} (user_id, max_cost) as select user_id, max(cost) as max_cost from ${tableName} group by user_id"

    qt_select """ select user_id, cost from ${tableName} order by user_id """
    qt_select """ select user_id, max_cost, "${viewName}" from ${viewName} order by user_id """

    sql """ INSERT INTO ${tableName} VALUES
            (1, '2017-10-01', 'Beijing', 10, 1, 1, 30, 20, hll_hash(1), to_bitmap(1))
        """
    sql """ INSERT INTO ${tableName} VALUES
            (1, '2017-10-02', 'Beijing', 10, 1, 1, 31, 19, hll_hash(2), to_bitmap(2))
        """
    sql """ sync """

    qt_select """ select * from ${tableName} order by user_id """

    qt_select """ select user_id, sum(cost) from ${tableName} group by user_id order by user_id """

    test {
        sql """ ALTER TABLE ${tableName} RENAME COLUMN user_id new_user_id """
        exception "errCode = 2,"
    }

    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-01', 'Beijing', 10, 1, 1, 31, 21, hll_hash(2), to_bitmap(2))
        """
    sql """ INSERT INTO ${tableName} VALUES
            (2, '2017-10-02', 'Beijing', 10, 1, 1, 32, 20, hll_hash(3), to_bitmap(3))
        """
    qt_desc """ desc ${tableName} """

    qt_select""" select * from ${tableName} order by user_id """
    qt_select """ select user_id, max_cost, "${viewName}" from ${viewName} order by user_id """

    qt_select """ select user_id, sum(cost) from ${tableName} group by user_id order by user_id """

    sql """ DROP TABLE ${tableName} """

}
