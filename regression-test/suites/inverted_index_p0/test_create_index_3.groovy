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


suite("test_create_index_3", "inverted_index"){
    // prepare test table
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def indexTbName1 = "test_create_index_3"
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
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // case 1: create table with index
    def create_index_result = "fail"
    try {
        sql """
                CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                    id INT DEFAULT '10',
                    name VARCHAR(32) DEFAULT '',
                    INDEX name_idx(name) USING INVERTED PROPERTIES("parse" = "english") COMMENT 'name index'
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """
        create_index_result = "success"
    } catch(Exception ex) {
        logger.info("typo for parser , result: " + ex)
    }
    assertEquals(create_index_result, "fail")

    // case 2: alter add index
    sql """
                CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                    id INT DEFAULT '10',
                    name VARCHAR(32) DEFAULT ''
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 10
                PROPERTIES("replication_num" = "1");
        """
    
    try {
        sql """
            create index name_idx on ${indexTbName1}(name) using inverted properties("parse" = "english") comment 'name index';
        """
        create_index_result = "success"
    } catch(Exception ex) {
        logger.info("typo for parser , result: " + ex)
    }
    assertEquals(create_index_result, "fail")

    sql """
            create index name_idx on ${indexTbName1}(name) using inverted properties("parser" = "english") comment 'name index';
        """
    
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "name_idx")
}
