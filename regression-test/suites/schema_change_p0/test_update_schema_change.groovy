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

suite ("test_update_schema_change") {

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    def tableName = "schema_change_update_test"

    def getAlterColumnJobState = { tbName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][9]
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
            `cost` BIGINT DEFAULT 0 COMMENT "用户总消费",
            `max_dwell_time` INT DEFAULT 0 COMMENT "用户最大停留时间",
            `min_dwell_time` INT DEFAULT 99999 COMMENT "用户最小停留时间")
        UNIQUE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        BUCKETS 8
        PROPERTIES ( "replication_num" = "1" , "light_schema_change" = "false");
        """

    sql """ 
        INSERT INTO ${tableName} VALUES
                (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20);
        """

    sql """
        INSERT INTO ${tableName} VALUES
                (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21);
        """

    qt_sc """ SELECT * FROM ${tableName} order by user_id ASC, last_visit_date; """

    // alter and test light schema change
    if (!isCloudMode()) {
        sql """ALTER TABLE ${tableName} SET ("light_schema_change" = "true");"""
    }

    sql """
        ALTER table ${tableName} ADD COLUMN new_column INT default 1;
        """

    def max_try_secs = 60
    while (max_try_secs--) {
        String result = getAlterColumnJobState(tableName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + result
                assertEquals("FINISHED", result)
            }
        }
    }

    qt_sc """ SELECT * FROM ${tableName} order by user_id DESC, last_visit_date; """

    sql """
        UPDATE ${tableName} set new_column = 2 where user_id = 1;
        """

    sql """ sync; """

    qt_sc """ SELECT * FROM ${tableName} order by user_id ASC, last_visit_date; """

    sql """
        INSERT INTO ${tableName} VALUES
                (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20, 2);
        """

    sql """
        INSERT INTO ${tableName} VALUES
                (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21, 20);
        """

    sql """
        UPDATE ${tableName} set new_column = 20 where new_column = 2;
        """

    sql """ sync; """

    qt_sc """ SELECT * FROM ${tableName} order by user_id DESC, last_visit_date; """

    sql """
        ALTER TABLE ${tableName} DROP COLUMN new_column;
        """

    max_try_secs = 60
    while (max_try_secs--) {
        String result = getAlterColumnJobState(tableName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + result
                assertEquals("FINISHED", result)
            }
        }
    }

    qt_sc """ SELECT * FROM ${tableName} order by user_id DESC, last_visit_date; """

    sql """
        UPDATE ${tableName} set cost = 20 where user_id = 5;
        """

    sql """ sync; """

    qt_sc """ SELECT * FROM ${tableName} order by user_id DESC, last_visit_date; """
}
