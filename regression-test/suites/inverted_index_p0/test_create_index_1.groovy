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


suite("test_create_index_1", "inverted_index"){
    // prepare test table
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def indexTbName1 = "test_create_index_1"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // case 1: create table with index
    // case 1.1: create duplicate same index for one colume with same name
    def create_dup_index_result = "fail"
    try {
        sql """
                CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                    name varchar(50),
                    age int NOT NULL,
                    grade int NOT NULL,
                    registDate datetime NULL,
                    studentInfo char(100),
                    tearchComment string,
                    INDEX age_idx(age) USING INVERTED COMMENT 'age index',
                    INDEX age_idx(age) USING INVERTED COMMENT 'age index'
                )
                DUPLICATE KEY(`name`)
                DISTRIBUTED BY HASH(`name`) BUCKETS 10
                properties("replication_num" = "1");
        """
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate same index for one colume with same name, result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")

    // case 1.2: create duplicate same index for one colume with different name
    try {
        sql """
                CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                    name varchar(50),
                    age int NOT NULL,
                    grade int NOT NULL,
                    registDate datetime NULL,
                    studentInfo char(100),
                    tearchComment string,
                    INDEX age_idx_1(age) USING INVERTED COMMENT 'age index',
                    INDEX age_idx_2(age) USING INVERTED COMMENT 'age index'
                )
                DUPLICATE KEY(`name`)
                DISTRIBUTED BY HASH(`name`) BUCKETS 10
                properties("replication_num" = "1");
        """
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate same index for one colume with different name, result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")

    // case 1.3: create duplicate different index for one colume with same name
    try {
        sql """
                CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                    name varchar(50),
                    age int NOT NULL,
                    grade int NOT NULL,
                    registDate datetime NULL,
                    studentInfo char(100),
                    tearchComment string,
                    INDEX age_idx(age) USING BITMAP COMMENT 'age index',
                    INDEX age_idx(age) USING INVERTED COMMENT 'age index'
                )
                DUPLICATE KEY(`name`)
                DISTRIBUTED BY HASH(`name`) BUCKETS 10
                properties("replication_num" = "1");
        """
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate different index for one colume with same name, result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")

    // case 1.4: create duplicate different index for one colume with different name
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                grade int NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string,
                INDEX age_idx_1(age) USING INVERTED COMMENT 'age index'
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """

    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "age_idx_1")
    
    // drop index
    sql "drop index age_idx_1 on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result.size(), 0)

    // case 2: alter add index
    sql "create index studentInfo_idx on ${indexTbName1}(studentInfo) using inverted"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "studentInfo_idx")
    // case 2.1: create duplicate same index for one colume with same name
    try {
        sql "create index studentInfo_idx on ${indexTbName1}(`studentInfo`) using inverted"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate same index for one colume with same name, result: " + ex)
    }

    // case 2.2: create duplicate same index for one colume with different name
    try {
        sql "create index studentInfo_idx_2 on ${indexTbName1}(`studentInfo`) using inverted"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate same index for one colume with different name, result: " + ex)
    }

    // case 2.3: create duplicate different index for one colume with same name
    try {
        sql "create index studentInfo_idx on ${indexTbName1}(`studentInfo`) using NGRAM_BF"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create duplicate different index for one colume with same name, result: " + ex)
    }

    // 2.4: create duplicate different index for one colume with different name
    sql "create index studentInfo_idx_2 on ${indexTbName1}(`studentInfo`) using NGRAM_BF"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 2)
    assertEquals(show_result[0][2], "studentInfo_idx")
    assertEquals(show_result[1][2], "studentInfo_idx_2")
}
