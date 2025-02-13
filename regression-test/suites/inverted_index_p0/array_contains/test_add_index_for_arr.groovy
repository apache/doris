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


suite("test_add_index_for_arr") {
    // prepare test table
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=false"""
    sql """ set enable_common_expr_pushdown=true """

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


    sql "DROP TABLE IF EXISTS my_test_array"
    // create table without any index
    sql """
            CREATE TABLE IF NOT EXISTS my_test_array (
                `id` int(11) NULL,
                `name` ARRAY<text> NULL,
                `description` ARRAY<text> NULL,
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            properties("replication_num" = "1");
    """

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // stream_load with csv data
     streamLoad {
             table "my_test_array"

             file "arr_null_test_data.csv" // import csv file
             time 10000 // limit inflight 10s
             set 'column_separator', '|'
             set 'format', 'csv'

             // if declared a check callback, the default check condition will ignore.
             // So you must check all condition
             check { result, exception, startTime, endTime ->
                 if (exception != null) {
                     throw exception
                 }
                 log.info("Stream load result: ${result}".toString())
                 def json = parseJson(result)
                 assertEquals(200, json.NumberTotalRows)
                 assertEquals(200, json.NumberLoadedRows)
                 assertTrue(json.LoadBytes > 0)
             }
     }

    // query without inverted index
    // query rows with array_contains
    def sql_query_name1 = sql "select id, name[1], description[1] from my_test_array where array_contains(name,'text7')"
    // query rows with !array_contains
    def sql_query_name2 = sql "select id, name[1], description[1] from my_test_array where !array_contains(name,'text7')"

    // add index for name
    sql "ALTER TABLE my_test_array ADD INDEX name_idx (name) USING INVERTED;"
    wait_for_latest_op_on_table_finish("my_test_array", timeout)
    // build index for name that name data can using inverted index
    if (!isCloudMode()) {
        sql "BUILD INDEX name_idx ON my_test_array"
        wait_for_build_index_on_partition_finish("my_test_array", timeout)
    }

    // query with inverted index
    sql "set enable_inverted_index_query=true"
    // query rows with array_contains
    def sql_query_name1_inverted = sql "select id, name[1], description[1] from my_test_array where array_contains(name,'text7')"
    // query rows with !array_contains
    def sql_query_name2_inverted = sql "select id, name[1], description[1] from my_test_array where !array_contains(name,'text7')"

    // check result for query without inverted index and with inverted index
    def size1 = sql_query_name1.size();
    log.info("sql_query_name1 query without inverted index rows size: ${size1}")
    for (int i = 0; i < sql_query_name1.size(); i++) {
        assertEquals(sql_query_name1[i][0], sql_query_name1_inverted[i][0])
        assertEquals(sql_query_name1[i][1], sql_query_name1_inverted[i][1])
        assertEquals(sql_query_name1[i][2], sql_query_name1_inverted[i][2])
    }
    def size2 = sql_query_name2.size();
    log.info("sql_query_name2 query without inverted index rows size: ${size2}")
    for (int i = 0; i < sql_query_name2.size(); i++) {
        assertEquals(sql_query_name2[i][0], sql_query_name2_inverted[i][0])
        assertEquals(sql_query_name2[i][1], sql_query_name2_inverted[i][1])
        assertEquals(sql_query_name2[i][2], sql_query_name2_inverted[i][2])
    }

    // drop index
    // add index on column description
    sql "drop index name_idx on my_test_array"
    wait_for_latest_op_on_table_finish("my_test_array", timeout)

    def sql_query_name1_without_inverted = sql "select id, name[1], description[1] from my_test_array where array_contains(name,'text7')"
    def sql_query_name2_without_inverted = sql "select id, name[1], description[1] from my_test_array where !array_contains(name,'text7')"

    assertEquals(sql_query_name1.size(), sql_query_name1_without_inverted.size())
    assertEquals(sql_query_name2.size(), sql_query_name2_without_inverted.size())
}
