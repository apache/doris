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


suite("test_add_drop_index", "inverted_index"){
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

    def indexTbName1 = "test_add_drop_inverted_index"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                grade int NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """
    

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // case1: create index for int colume
    // case1.0 create index
    sql "create index age_idx on ${indexTbName1}(tearchComment) using inverted"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    def show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[0][2], "age_idx")
    
    // case1.1 create duplicate same index for one colume with same name
    def create_dup_index_result = "fail"
    try {
        sql "create index age_idx on ${indexTbName1}(`tearchComment`) using inverted"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create same duplicate and same name index,  result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")
    // case1.2 create duplicate same index for one colume with different name
    try {
        sql "create index age_idx_diff on ${indexTbName1}(`tearchComment`) using inverted"
        create_dup_index_result = "success"
    } catch(Exception ex) {
        logger.info("create same duplicate with different name index,  result: " + ex)
    }
    assertEquals(create_dup_index_result, "fail")
    // case1.3 create duplicate different index for one colume with different name
    sql "create index age_idx_diff on ${indexTbName1}(`tearchComment`) using NGRAM_BF"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    logger.info("show index from " + indexTbName1 + " result: " + show_result)
    assertEquals(show_result[1][2], "age_idx_diff")
    
    // case1.4 drop index
    def drop_result = sql """
                          ALTER TABLE ${indexTbName1}
                              drop index age_idx,
                              drop index age_idx_diff;
                      """
    logger.info("drop index age_idx and age_idx_diff on " + indexTbName1 + "; result: " + drop_result)
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result.size(), 0)
    
    // case1.5 drop index again, expect drop fail
    def drop_index_twice_result = "fail"
    try {
        drop_result = sql "drop index age_idx on ${indexTbName1}"
        drop_index_twice_result = "success"
    } catch(Exception ex) {
        logger.info("drop index again, result: " + ex)
    }
    assertEquals(drop_index_twice_result, "fail")

    // case2: create index for date colume
    sql "create index date_idx on ${indexTbName1}(`registDate`) using inverted"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result.size(), 1)
    sql "drop index date_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case3: create string inverted index for int colume
    def create_string_index_on_int_colume_result = "fail"
    try {
        syntax_error = sql "create index age_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES("parser"="standard")"
        create_string_index_on_int_colume_result = "success"
    } catch(Exception ex) {
        logger.info("sql: create index age_idx on" + indexTbName1 + "(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")")
        logger.info("create string inverted index for int colume, result: " + ex)
    }
    assertEquals(create_string_index_on_int_colume_result, "fail")

    // case4: create default inverted index for varchar coulume
    sql "create index name_idx on ${indexTbName1}(`name`) using inverted"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "name_idx" && show_result[0][10] == "INVERTED")
    logger.info("create index name_idx for " + indexTbName1 + "(`name`)")
    logger.info("show index result: " + show_result)
    sql "drop index name_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case5: create none inverted index for char colume
    sql "create index name_idx on ${indexTbName1}(`name`) USING INVERTED PROPERTIES(\"parser\"=\"none\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertEquals(show_result[0][10], "INVERTED")
    logger.info("create index name_idx for " + indexTbName1 + "(`name`) USING INVERTED PROPERTIES(\"parser\"=\"none\")")
    logger.info("show index result: " + show_result)
    sql "drop index name_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case5：create simple inverted index for char colume
    sql "create index studentInfo_idx on ${indexTbName1}(`studentInfo`) USING INVERTED PROPERTIES(\"parser\"=\"english\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "studentInfo_idx" && show_result[0][10] == "INVERTED")
    logger.info("create index studentInfo_idx for " + indexTbName1 + "(`studentInfo`) USING INVERTED PROPERTIES(\"parser\"=\"english\")")
    logger.info("show index result: " + show_result)
    sql "drop index studentInfo_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case6: create standard inverted index for text colume
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    logger.info("create index tearchComment_idx for " + indexTbName1 + "(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")")
    logger.info("show index result: " + show_result)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case7: drop not exist index
    def drop_no_exist_index_result = "fail"
    try {
        sql "drop index no_exist_idx on ${indexTbName1}"
        drop_no_exist_index_result = "success"
    } catch(Exception ex) {
       logger.info("drop not exist index: result " + ex)
    }
    assertEquals(drop_no_exist_index_result, "fail")

    // case8: create，drop, create index
    // case8.0: create, drop, create same index with same name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case8.1: create, drop, create other index with same name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"none\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

    // case8.2: create, drop, create same index with other name
    sql "create index tearchComment_idx on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "drop index tearchComment_idx on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    sql "create index tearchComment_idx_2 on ${indexTbName1}(`tearchComment`) USING INVERTED PROPERTIES(\"parser\"=\"standard\")"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)
    show_result = sql "show index from ${indexTbName1}"
    assertTrue(show_result[0][2] == "tearchComment_idx_2" && show_result[0][10] == "INVERTED")
    sql "drop index tearchComment_idx_2 on ${indexTbName1}"
    wait_for_latest_op_on_table_finish(indexTbName1, timeout)

}
