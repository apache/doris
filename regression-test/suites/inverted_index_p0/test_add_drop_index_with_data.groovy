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


suite("test_add_drop_index_with_data", "inverted_index"){
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
                 break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout)
    }

    def indexTbName1 = "test_add_drop_inverted_index2"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                `id` int(11) NULL,
                `name` text NULL,
                `description` text NULL,
                INDEX idx_id (`id`) USING INVERTED COMMENT '',
                INDEX idx_name (`name`) USING INVERTED PROPERTIES("parser"="none") COMMENT ''
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            properties("replication_num" = "1");
    """

    // set enable_vectorized_engine=true
    sql """ SET enable_vectorized_engine=true; """
    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // show index of create table
    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")

    // insert data
    sql "insert into ${indexTbName1} values (1, 'name1', 'desc 1'), (2, 'name2', 'desc 2')"

    // query all rows
    def select_result = sql "select * from ${indexTbName1} order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where name='name1'
    select_result = sql "select * from ${indexTbName1} where name='name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql "select * from ${indexTbName1} where name='name2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // query rows where description match 'desc', should fail without index
    def success = false
    try {
        sql "select * from ${indexTbName1} where description match 'desc'"
        success = true
    } catch(Exception ex) {
        logger.info("sql exception: " + ex)
    }
    assertEquals(success, false)

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // show index after add index
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[2][2], "idx_desc")

    // query rows where description match 'desc'
    select_result = sql "select * from ${indexTbName1} where description match 'desc' order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop index
    // add index on column description
    sql "drop index idx_desc on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where description match 'desc', should fail without index
    success = false
    try {
        sql "select * from ${indexTbName1} where description match 'desc'"
        success = true
    } catch(Exception ex) {
        logger.info("sql exception: " + ex)
    }
    assertEquals(success, false)

    // query rows where name='name1'
    select_result = sql "select * from ${indexTbName1} where name='name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql "select * from ${indexTbName1} where name='name2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop idx_id index
    sql "drop index idx_id on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // show index of create table
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "idx_name")

    // query rows where name match 'name1'
    select_result = sql "select * from ${indexTbName1} where name match 'name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name match 'name2'
    select_result = sql "select * from ${indexTbName1} where name match 'name2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop idx_id index
    sql "drop index idx_name on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where name match 'name1', should fail without index
    success = false
    try {
        sql "select * from ${indexTbName1} where name match 'name1'"
        success = true
    } catch(Exception ex) {
        logger.info("sql exception: " + ex)
    }
    assertEquals(success, false)

    // show index of create table
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 0)

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where description match 'desc'
    select_result = sql "select * from ${indexTbName1} where description match 'desc' order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql "select * from ${indexTbName1} where description match_all 'desc 2'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")
}