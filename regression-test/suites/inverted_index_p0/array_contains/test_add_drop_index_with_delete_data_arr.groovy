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


suite("test_add_drop_index_with_delete_data_arr", "array_contains_inverted_index"){
    // prepare test table
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

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

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    def indexTbName1 = "test_add_drop_inverted_index3_arr"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                `id` int(11) NULL,
                `name` ARRAY<text> NULL,
                `description` ARRAY<text> NULL,
                INDEX idx_id (`id`) USING INVERTED COMMENT '',
                INDEX idx_name (`name`) USING INVERTED PROPERTIES("parser"="none") COMMENT ''
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            properties("replication_num" = "1");
    """

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // show index of create table
    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")

    // insert data
    sql """ insert into ${indexTbName1} VALUES
        (1, ['name1'], ['desc test hello']),
        (2, ['name2'], ['desc hello ok']),
        (3, ['name3'], ['hello world']),
        (4, ['name4'], ['desc ok world test']),
        (5, ['name5'], ['desc world'])
    """

    def sql_query_desc = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description, 'desc test hello') OR " +
            "array_contains(description, 'desc hello ok') OR array_contains(description, 'desc ok world test') OR array_contains(description, 'desc world') order by id"
    def sql_query_hello = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description, 'hello') order by id"
    def sql_query_world = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description, 'hello world') OR " +
            "array_contains(description, 'desc ok world test') OR array_contains(description,'desc world')  order by id"
    def sql_query_ok = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description, 'desc hello ok')" +
            "OR array_contains(description, 'desc ok world test') order by id"

    // query all rows
    def select_result = sql "select id, name[1], description[1] from ${indexTbName1} order by id"
    assertEquals(select_result.size(), 5)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc hello ok")
    assertEquals(select_result[2][0], 3)
    assertEquals(select_result[2][1], "name3")
    assertEquals(select_result[2][2], "hello world")
    assertEquals(select_result[3][0], 4)
    assertEquals(select_result[3][1], "name4")
    assertEquals(select_result[3][2], "desc ok world test")
    assertEquals(select_result[4][0], 5)
    assertEquals(select_result[4][1], "name5")
    assertEquals(select_result[4][2], "desc world")

    // query rows where description match 'desc', should fail without index
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 4)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc hello ok")
    assertEquals(select_result[2][0], 4)
    assertEquals(select_result[2][1], "name4")
    assertEquals(select_result[2][2], "desc ok world test")
    assertEquals(select_result[3][0], 5)
    assertEquals(select_result[3][1], "name5")
    assertEquals(select_result[3][2], "desc world")

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    if (!isCloudMode()) {
        sql "build index idx_desc on ${indexTbName1}"
        wait_for_build_index_on_partition_finish(indexTbName1, timeout)
    }

    // show index after add index
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[2][2], "idx_desc")

    // query rows where description match 'desc'
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 4)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc hello ok")
    assertEquals(select_result[2][0], 4)
    assertEquals(select_result[2][1], "name4")
    assertEquals(select_result[2][2], "desc ok world test")
    assertEquals(select_result[3][0], 5)
    assertEquals(select_result[3][1], "name5")
    assertEquals(select_result[3][2], "desc world")

    // query rows where description match 'hello'
    select_result = sql sql_query_hello
    assertEquals(select_result.size(), 0)

    select_result = sql sql_query_ok
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc hello ok")
    assertEquals(select_result[1][0], 4)
    assertEquals(select_result[1][1], "name4")
    assertEquals(select_result[1][2], "desc ok world test")

    // drop index
    sql "drop index idx_desc on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    // query rows where description match 'desc' without index
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 4)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc hello ok")
    assertEquals(select_result[2][0], 4)
    assertEquals(select_result[2][1], "name4")
    assertEquals(select_result[2][2], "desc ok world test")
    assertEquals(select_result[3][0], 5)
    assertEquals(select_result[3][1], "name5")
    assertEquals(select_result[3][2], "desc world")

    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")

    // delete row
    sql "delete from ${indexTbName1} where id=2"
    sql "delete from ${indexTbName1} where id=4"

    // query all rows
    select_result = sql "select id, name[1], description[1] from ${indexTbName1} order by id"
    assertEquals(select_result.size(), 3)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 3)
    assertEquals(select_result[1][1], "name3")
    assertEquals(select_result[1][2], "hello world")
    assertEquals(select_result[2][0], 5)
    assertEquals(select_result[2][1], "name5")
    assertEquals(select_result[2][2], "desc world")
    
    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    if (!isCloudMode()) {
        sql "build index idx_desc on ${indexTbName1}"
        wait_for_build_index_on_partition_finish(indexTbName1, timeout)
    }

    // show index after add index
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[2][2], "idx_desc")

    // query rows where description match 'desc'
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc test hello")
    assertEquals(select_result[1][0], 5)
    assertEquals(select_result[1][1], "name5")
    assertEquals(select_result[1][2], "desc world")

    // query rows where description match 'hello'
    select_result = sql sql_query_hello
    assertEquals(select_result.size(), 0)

    // query rows where description match 'world'
    select_result = sql sql_query_world
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 3)
    assertEquals(select_result[0][1], "name3")
    assertEquals(select_result[0][2], "hello world")
    assertEquals(select_result[1][0], 5)
    assertEquals(select_result[1][1], "name5")
    assertEquals(select_result[1][2], "desc world")

    // query rows where description match 'ok'
    select_result = sql sql_query_ok
    assertEquals(select_result.size(), 0)
}
