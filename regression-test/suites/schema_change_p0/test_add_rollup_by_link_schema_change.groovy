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

suite ("test_add_rollup_by_link_schema_change") {
    def aggTableName = "test_add_rollup_by_link_schema_change_agg"
    def dupTableName = "test_add_rollup_by_link_schema_change_dup"
    def uniTableName = "test_add_rollup_by_link_schema_change_uni"

    /* agg */
    sql """ DROP TABLE IF EXISTS ${aggTableName} FORCE"""
    sql """
            CREATE TABLE IF NOT EXISTS ${aggTableName} (
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
            BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );
        """

    // add materialized view (failed)
    def result = "null"
    def mvName = "mv1"
    sql "create materialized view ${mvName} as select user_id, date, city, age, sex, sum(cost) from ${aggTableName} group by user_id, date, city, age, sex;"
    while (!result.contains("CANCELLED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${aggTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("FINISHED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    sql "sync"

    //add rollup (failed)
    result = "null"
    def rollupName = "rollup_cost"
    sql "ALTER TABLE ${aggTableName} ADD ROLLUP ${rollupName}(`user_id`,`date`,`city`,`age`, `sex`, cost);"
    while (!result.contains("CANCELLED")){
        result = sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${aggTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("FINISHED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    sql "sync"

    // add materialized view (success)
    // Keys are less than the original table
    result = "null"
    mvName = "mv1"
    sql "create materialized view ${mvName} as select user_id, date, city, age, sum(cost) from ${aggTableName} group by user_id, date, city, age;"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${aggTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    sql "sync"

    // add rollup (success)
    // The order of the keys is reversed
    result = "null"
    rollupName = "rollup_cost"
    sql "ALTER TABLE ${aggTableName} ADD ROLLUP ${rollupName}(`date`,`user_id`,`city`,`age`, `sex`, cost);"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${aggTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    sql "sync"

    /* dup */
    sql """ DROP TABLE IF EXISTS ${dupTableName} FORCE"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dupTableName} (
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
        BUCKETS 1
        PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" );
    """

    //add materialized view (success)
    result = "null"
    mvName = "mv1"
    sql "create materialized view ${mvName} as select user_id, date, city, age, sex, sum(cost) from ${dupTableName} group by user_id, date, city, age, sex;"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${dupTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    sql "sync"

    //add rollup (failed)
    result = "null"
    rollupName = "rollup_cost"
    sql "ALTER TABLE ${dupTableName} ADD ROLLUP ${rollupName}(`user_id`,`date`,`city`,`age`,`sex`) DUPLICATE KEY (`user_id`,`date`,`city`,`age`,`sex`);"
    while (!result.contains("CANCELLED")){
        result = sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${dupTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("FINISHED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }

    /* unique */
    sql """ DROP TABLE IF EXISTS ${uniTableName} FORCE"""
    sql """
        CREATE TABLE IF NOT EXISTS ${uniTableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `city` VARCHAR(20) DEFAULT "beijing "COMMENT "用户所在城市",
                `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
            UNIQUE KEY(`user_id`, `date`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true");
    """

    result = "null"
    rollupName = "rollup_cost"
    sql "ALTER TABLE ${uniTableName} ADD ROLLUP ${rollupName}(`user_id`,`date`,`age`, `sex`, cost);"
    while (!result.contains("CANCELLED")){
        result = sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${uniTableName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("FINISHED")){
            assertTrue(false);
        }
        Thread.sleep(100)
    }
}
