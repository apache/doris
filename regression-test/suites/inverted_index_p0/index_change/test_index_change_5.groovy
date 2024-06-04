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

suite("test_index_change_5") {
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }
    
    def tableName = "test_index_change_5"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `note` TEXT COMMENT "备注",
            INDEX idx_user_id (`user_id`) USING INVERTED COMMENT '',
            INDEX idx_note (`note`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1" );
        """

    sql """ INSERT INTO ${tableName} VALUES
         (1, '2017-10-01', 'Beijing China', 10, 1, 'Software Developer')
        """
    
    sql """ INSERT INTO ${tableName} VALUES
         (2, '2017-10-01', 'Beijing China', 10, 1, 'Communication Engineer')
        """
    
    sql """ INSERT INTO ${tableName} VALUES
         (3, '2017-10-01', 'Shanghai China', 10, 1, 'electrical engineer')
        """
    
    sql """ INSERT INTO ${tableName} VALUES
         (4, '2017-10-02', 'Beijing China', 10, 0, 'Both a teacher and a scientist')
        """
    
    sql """ INSERT INTO ${tableName} VALUES
         (5, '2017-10-02', 'Shenzhen China', 10, 1, 'teacher')
        """
    
    sql """ INSERT INTO ${tableName} VALUES
         (6, '2017-10-03', 'Hongkong China', 10, 1, 'Architectural designer')
        """
    
    qt_select1 """ SELECT * FROM ${tableName} t ORDER BY user_id,date,city,age,sex; """
    qt_select2 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' ORDER BY user_id; """
    qt_select3 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' and sex = 1 ORDER BY user_id; """
    qt_select4 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer' and sex = 0 ORDER BY user_id; """
    qt_select5 """ SELECT * FROM ${tableName} t WHERE note MATCH_PHRASE 'electrical engineer' ORDER BY user_id; """
    qt_select6 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer Developer' AND city match_all 'Shanghai China' ORDER BY user_id; """

    // drop inverted index idx_user_id, idx_note
    sql """ DROP INDEX idx_user_id ON ${tableName} """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    sql """ DROP INDEX idx_note ON ${tableName} """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    // create bitmap index
    sql """ CREATE INDEX idx_sex ON ${tableName}(`sex`) USING BITMAP """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    // wait_for_build_index_on_partition_finish(tableName, timeout)

    def show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "idx_sex")

    qt_select7 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' ORDER BY user_id; """
    qt_select8 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' and sex = 1 ORDER BY user_id; """
    qt_select9 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer' and sex = 0 ORDER BY user_id; """
    qt_select10 """ SELECT * FROM ${tableName} t WHERE note MATCH_PHRASE 'electrical engineer' ORDER BY user_id; """
    qt_select11 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer Developer' AND city match_all 'Shanghai China' ORDER BY user_id; """

    tableName = "test_index_change_5_v1"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `note` TEXT COMMENT "备注",
            INDEX idx_user_id (`user_id`) USING INVERTED COMMENT '',
            INDEX idx_note (`note`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
        PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = "V1" );
        """

    sql """ INSERT INTO ${tableName} VALUES
         (1, '2017-10-01', 'Beijing China', 10, 1, 'Software Developer')
        """

    sql """ INSERT INTO ${tableName} VALUES
         (2, '2017-10-01', 'Beijing China', 10, 1, 'Communication Engineer')
        """

    sql """ INSERT INTO ${tableName} VALUES
         (3, '2017-10-01', 'Shanghai China', 10, 1, 'electrical engineer')
        """

    sql """ INSERT INTO ${tableName} VALUES
         (4, '2017-10-02', 'Beijing China', 10, 0, 'Both a teacher and a scientist')
        """

    sql """ INSERT INTO ${tableName} VALUES
         (5, '2017-10-02', 'Shenzhen China', 10, 1, 'teacher')
        """

    sql """ INSERT INTO ${tableName} VALUES
         (6, '2017-10-03', 'Hongkong China', 10, 1, 'Architectural designer')
        """

    qt_select1_v1 """ SELECT * FROM ${tableName} t ORDER BY user_id,date,city,age,sex; """
    qt_select2_v1 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' ORDER BY user_id; """
    qt_select3_v1 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' and sex = 1 ORDER BY user_id; """
    qt_select4_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer' and sex = 0 ORDER BY user_id; """
    qt_select5_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH_PHRASE 'electrical engineer' ORDER BY user_id; """
    qt_select6_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer Developer' AND city match_all 'Shanghai China' ORDER BY user_id; """

    // drop inverted index idx_user_id, idx_note
    sql """ DROP INDEX idx_user_id ON ${tableName} """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    sql """ DROP INDEX idx_note ON ${tableName} """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    // create bitmap index
    sql """ CREATE INDEX idx_sex ON ${tableName}(`sex`) USING BITMAP """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    // wait_for_build_index_on_partition_finish(tableName, timeout)

    show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "idx_sex")

    qt_select7_v1 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' ORDER BY user_id; """
    qt_select8_v1 """ SELECT * FROM ${tableName} t WHERE city MATCH 'beijing' and sex = 1 ORDER BY user_id; """
    qt_select9_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer' and sex = 0 ORDER BY user_id; """
    qt_select10_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH_PHRASE 'electrical engineer' ORDER BY user_id; """
    qt_select11_v1 """ SELECT * FROM ${tableName} t WHERE note MATCH 'engineer Developer' AND city match_all 'Shanghai China' ORDER BY user_id; """
}
