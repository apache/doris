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

    sql "set enable_add_index_for_new_data = true"

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
    select_result = sql "select * from ${indexTbName1} where description match 'desc' order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_last_col_change_finish(indexTbName1, timeout)

    if(!isCloudMode()) {
        run_index_change_job_and_wait(indexTbName1, timeout) {
            build_index_on_table("idx_desc", indexTbName1)
        }
    }

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
    run_index_change_job_and_wait(indexTbName1, timeout) {
        sql "drop index idx_desc on ${indexTbName1}"
    }

    // query rows where description match 'desc', should fail without index
    select_result = sql "select * from ${indexTbName1} where description match 'desc' order by id"
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

    // drop idx_id index
    run_index_change_job_and_wait(indexTbName1, timeout) {
        sql "drop index idx_id on ${indexTbName1}"
    }

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

    // drop idx_name index
    run_index_change_job_and_wait(indexTbName1, timeout) {
        sql "drop index idx_name on ${indexTbName1}"
    }

    // query rows where name match 'name1' without index
    select_result = sql "select * from ${indexTbName1} where name match 'name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // show index of create table
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 0)

    // add index on column description
    sql "create index idx_desc on ${indexTbName1}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\");"
    wait_for_last_col_change_finish(indexTbName1, timeout)
    if (!isCloudMode()) {
        run_index_change_job_and_wait(indexTbName1, timeout) {
            build_index_on_table("idx_desc", indexTbName1)
        }
    }

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

    // alter table add multiple index
    select_result = sql """
                        ALTER TABLE ${indexTbName1}
                            ADD INDEX idx_id (id) USING INVERTED,
                            ADD INDEX idx_name (name) USING INVERTED;
                    """

    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 3)

    if (isCloudMode()) {
        // Cloud BUILD INDEX is table-scoped and builds both newly added indexes.
        run_index_change_job_and_wait(indexTbName1, timeout) {
            build_index_on_table("idx_name", indexTbName1)
        }
    } else {
        run_index_change_job_and_wait(indexTbName1, timeout) {
            build_index_on_table("idx_id", indexTbName1)
        }
        run_index_change_job_and_wait(indexTbName1, timeout) {
            build_index_on_table("idx_name", indexTbName1)
        }
    }

    def physical_id_result = sql """SELECT /*+ SET_VAR(enable_fallback_on_missing_inverted_index=false) */ id
            FROM ${indexTbName1} WHERE id = 1"""
    assertEquals(1, physical_id_result.size())
    assertEquals(1, physical_id_result[0][0])
    def physical_name_result = sql """SELECT /*+ SET_VAR(enable_fallback_on_missing_inverted_index=false) */ id
            FROM ${indexTbName1} WHERE name MATCH 'name1'"""
    assertEquals(1, physical_name_result.size())
    assertEquals(1, physical_name_result[0][0])

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


    // alter table drop multiple index
    run_index_change_job_and_wait(indexTbName1, timeout) {
        select_result = sql """
                            ALTER TABLE ${indexTbName1}
                                DROP INDEX idx_id,
                                DROP INDEX idx_name,
                                DROP INDEX idx_desc;
                        """
    }

    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result.size(), 0)

    // query rows where name match 'name1' without index
    select_result = sql "select * from ${indexTbName1} where name match 'name1'"
    assertEquals(select_result.size(), 1)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")

    // query rows where description match 'desc' without index
    select_result = sql "select * from ${indexTbName1} where description match 'desc' order by id"
    assertEquals(select_result.size(), 2)
    assertEquals(select_result[0][0], 1)
    assertEquals(select_result[0][1], "name1")
    assertEquals(select_result[0][2], "desc 1")
    assertEquals(select_result[1][0], 2)
    assertEquals(select_result[1][1], "name2")
    assertEquals(select_result[1][2], "desc 2")
}
