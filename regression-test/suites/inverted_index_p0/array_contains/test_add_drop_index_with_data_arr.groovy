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


suite("test_add_drop_index_with_data_arr", "array_contains_inverted_index") {
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

    def indexTbName1 = "test_add_drop_inverted_index2_arr"

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

    // query rows where name='name1'
    def sql_query_name1 = "select id, name[1], description[1] from ${indexTbName1} where array_contains(name,'name1')"
    // query rows where name='name2'
    def sql_query_name2 = "select id, name[1], description[1] from ${indexTbName1} where array_contains(name,'name2')"
    // query rows where description match 'desc 1'
    def sql_query_desc1 = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description,'desc 1')"
    // query rows where description match 'desc 2'
    def sql_query_desc2 = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description,'desc 2')"
    // query rows where description match 'desc'
    def sql_query_desc = "select id, name[1], description[1] from ${indexTbName1} where array_contains(description,'desc 1') OR array_contains(description,'desc 2') order by id"

    // show index of create table
    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[1][2], "idx_name")

    // insert data
    sql "insert into ${indexTbName1} values (1, ['name1'], ['desc 1']), (2, ['name2'], ['desc 2'])"

    // query all rows
    def select_result = sql "select id, name[1], description[1] from ${indexTbName1} order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where name='name1'
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql sql_query_name2
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // query rows where description match 'desc', should fail without index
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"none\");"
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
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql sql_query_desc1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql sql_query_desc2
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop index
    // add index on column description
    sql "drop index idx_desc on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where description match 'desc', should fail without index
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where name='name1'
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name='name2'
    select_result = sql sql_query_name2
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
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name match 'name2'
    select_result = sql sql_query_name2
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // drop idx_name index
    sql "drop index idx_name on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // query rows where name match 'name1' without index
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // show index of create table
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 0)

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"none\");"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    if (!isCloudMode()) {
        sql "build index idx_desc on ${indexTbName1}"
        wait_for_build_index_on_partition_finish(indexTbName1, timeout)
    }
    // query rows where description match 'desc'
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // query rows where description match_all 'desc 1'
    select_result = sql sql_query_desc1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match_all 'desc 2'
    select_result = sql sql_query_desc2
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")

    // alter table add multiple index
    select_result = sql """
                        ALTER TABLE ${indexTbName1}
                            ADD INDEX idx_id (id) USING INVERTED,
                            ADD INDEX idx_name (name) USING INVERTED;
                    """
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 3)
    if (!isCloudMode()) {
        sql "build index idx_name on ${indexTbName1}"
        wait_for_build_index_on_partition_finish(indexTbName1, timeout)
    }

    // query rows where name match 'name1'
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where name match 'name2'
    select_result = sql sql_query_name2
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 2)
    assertEquals(select_result[0][1], "name2")
    assertEquals(select_result[0][2], "desc 2")


    // alter table drop multiple index
    select_result = sql """
                        ALTER TABLE ${indexTbName1}
                            DROP INDEX idx_id,
                            DROP INDEX idx_name,
                            DROP INDEX idx_desc;
                    """
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 0)

    // query rows where name match 'name1' without index
    select_result = sql sql_query_name1
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match 'desc' without index
    select_result = sql sql_query_desc
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")
}
