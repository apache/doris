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

suite("test_dynamic_table", "dynamic_table"){
    // prepare test table

    def load_json_data = {table_name, vec_flag, format_flag, read_flag, file_name, expect_success ->
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'read_json_by_line', read_flag
            
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                if (expect_success == "false") {
                    assertEquals("fail", json.Status.toLowerCase())
                } else {
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }
    /*
   // case0: test simple dat steam load to dynamic table
    def TbName0 = "test_dytable_alter_colume_type"
    sql "DROP TABLE IF EXISTS ${TbName0}"
    sql """
        CREATE TABLE IF NOT EXISTS ${TbName0} (
                name char(50),
                ...
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
        """

    // case0.0: dirty data, which not influence table shchema
    // contact.phone colume except that the last line is string, all other lines are int
    load_json_data.call(TbName0, 'true', 'json', 'true', 'dynamic_simple_type_conflic_data.json', 'false')
    def desc_result_now = sql "desc ${TbName0}"
    logger.info("after stream load dirty data, desc result: ${desc_result_now}".toString())
    for(int i = 0; i < desc_result_now.size(); i++) {
        if(desc_result_now[i][0] == "contact.phone"){
            assertEquals(desc_result_now[i][1], "BIGINT")
        }
    }

    // case0.1: stream load data in consistent format
    load_json_data.call(TbName0, 'true', 'json', 'true', 'dynamic_bigint_contact_phone.json', 'true')
    def select_res = sql "select * from ${TbName0}"
    

    // case0.2 alter schema from int to string； and then load data
    sql """
        ALTER TABLE ${TbName0}
        MODIFY COLUMN `contact.phone` STRING;
        """

    for(i = 0; i < 5; i++){
        select_res = sql "select * from ${TbName0}"
        logger.info("after alter schema, it's ${i} select,  select result: ${select_res}".toString())
        sleep(3000)
    }

    def TbName1 = "test_complex_json_dytable"
    sql "DROP TABLE IF EXISTS ${TbName1}"
    // create 1 replica table
    def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${TbName1} (
                test_id int,
                `test.name` array<string>,
                ...
            )
            DUPLICATE KEY(`test_id`)
            DISTRIBUTED BY HASH(`test_id`) BUCKETS 10
            properties("replication_num" = "1");
        """
    // DDL/DML return 1 row and 3 column, the only value is update row count
    assertTrue(result1.size() == 1)
    assertTrue(result1[0].size() == 1)
    assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

    //insert data as common table
    sql """
        insert into ${TbName1} values (1, ["zhangsan", "lisi"]);
        """
    result1 = sql """select * from  ${TbName1} """
    logger.info("show index from " + result1)

    // stream load data
    */

    def real_alter_res = "true"
    def res = "null"
    def wait_for_alter_finish = "null"
    def check_time = 30
    def test_alter_colume_type_from_srcType_to_dstType = { table_name, colume_name, src_json, src_type, dst_type, dst_json, expect_alter_success ->
        //create table
        sql "DROP TABLE IF EXISTS ${table_name} FORCE"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                name char(50),
                `${colume_name}` ${src_type},
                ...
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
        """

        //stream load src_json
        load_json_data.call(table_name, 'true', 'json', 'true', src_json, 'true')
        sleep(1000)
        def select_res = sql "select * from ${table_name} order by name"
        logger.info("after stream load ${table_name}, select result: ${select_res}".toString())
        // check src_type
        def desc_result_now = sql "desc ${table_name}"
        for(int i = 0; i < desc_result_now.size(); i++) {
            if(desc_result_now[i][0] == colume_name){
                assertEquals(desc_result_now[i][1], src_type)
            }
        }

        //alter colume type from src_type to dst_type
        logger.info("ALTER TABLE ${table_name} MODIFY COLUMN `${colume_name}` ${dst_type}".toString())
        try {
            check_time = 30
            res = sql """
            ALTER TABLE ${table_name}
            MODIFY COLUMN `${colume_name}` ${dst_type};
            """
            //logger.info("alter res: ${res}".toString())
            while (!wait_for_alter_finish.contains("FINISHED") && check_time <= 30){
                wait_for_alter_finish = sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${table_name}' ORDER BY CreateTime DESC LIMIT 1; """
                wait_for_alter_finish = wait_for_alter_finish.toString()
                logger.info("check alter result: ${wait_for_alter_finish}".toString())
                if (wait_for_alter_finish.contains("CANCELLED") || check_time < 0 ){
                    real_alter_res = "false"
                    break
                }
                sleep(1000)
                check_time = check_time - 1
            }
            
        } catch(Exception ex) {
            logger.info("alter ${table_name} ${colume_name} from ${src_type} to ${dst_type}, catch exception: ${ex}".toString())
            real_alter_res = "false"
        } finally{
            logger.info("alter res: ${res}".toString())
            assertEquals(expect_alter_success, real_alter_res)
            if(expect_alter_success == "false"){
                logger.info("${table_name} expect fail")
                return
            }
        }

        //check data in table and check table schema
        def select_res_now = "true"
        for(i = 0; i < 5; i++){
            select_res_now = sql "select * from ${table_name} order by name"
            logger.info("after alter schema, it's ${i} time select,  select result: ${select_res_now}".toString())
            assertEquals(select_res.toString(), select_res_now.toString())
            sleep(3000)
        }

        desc_result_now = sql "desc ${table_name}"
        for(int i = 0; i < desc_result_now.size(); i++) {
            if(desc_result_now[i][0] == colume_name){
                assertEquals(desc_result_now[i][1], dst_type)
            }
        }

        // stream load data again
        load_json_data.call(table_name, 'true', 'json', 'true', dst_json, 'true')
    }
     
    //case1: test BIGINT transfer STRING， expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_BIGINT_to_STRING", "contact.phone", "dynamic_bigint_contact_phone.json", "BIGINT", "TEXT", "dynamic_string_contact_phone.json", "true")
    //case2: test DATE transfer STRING， expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_DATE_to_STRING", "contact.birthday", "dynamic_date_birthday.json", "DATE", "TEXT", "dynamic_string_birthday.json", "false")
    //case3: test TEXT transfer DATE, expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_STRING_to_DATE", "contact.birthday", "dynamic_string_birthday.json", "TEXT", "DATE", "dynamic_date_birthday.json", "false")
    //case4: test INT transfer DATE, expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_INT_to_DATE", "contact.birthday", "dynamic_timestamp_birthday.json", "INT", "DATE", "dynamic_date_birthday.json", "false")
    //case5: test DATE transfer INT, expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_DATE_to_INT", "contact.birthday", "dynamic_date_birthday.json", "DATE", "INT", "dynamic_timestamp_birthday.json", "false")
    //case5: test DATE(stream load timestamp) transfer STRINF, expect false
    test_alter_colume_type_from_srcType_to_dstType("test_dytable_alter_TimeStampDATE_to_STRING", "contact.birthday", "dynamic_timestamp_birthday.json", "DATE", "TEXT", "dynamic_string_birthday.json", "false")


}
