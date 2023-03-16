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

    def real_res = "true"
    def res = "null"
    def wait_for_alter_finish = "null"
    def check_time = 30
    def colume_set = ""
    def key = ""
    def test_create_and_load_dynamic_table = { table_name, data_model, replica_num, columes, columes_type, src_json, expect_success ->
        //create table
        sql "DROP TABLE IF EXISTS ${table_name}"
        colume_set = ""
        key = ""
        for (def col=0; col<columes.size(); col++){
            if(columes[col].contains(".")){
                colume_set += "`${columes[col]}` ${columes_type[col]}, "
                key += "`${columes[col]}`"
            }else{
                colume_set += "${columes[col]} ${columes_type[col]}, "
                key += "${columes[col]}"
            }
            if(col < columes.size() - 1){
                key += ", "
            }
        }

        try {
            sql """
                CREATE TABLE IF NOT EXISTS ${table_name} (
                    ${colume_set}
                    ...
                )
                ${data_model} KEY(${key})
                DISTRIBUTED BY HASH(`${columes[0]}`) BUCKETS 10
                properties("replication_num" = "${replica_num}");
            """
        }catch(Exception ex) {
            logger.info("create ${table_name} fail, catch exception: ${ex}".toString())
            real_res = "false"
        }finally{
            assertEquals(expect_success, real_res)
            if(expect_success == "false"){
                logger.info("${table_name} expect fail")
                return
            }
        }

        //stream load src_json
        load_json_data.call(table_name, 'true', 'json', 'true', src_json, 'true')
        sleep(1000)
        //def select_res = sql "select * from ${table_name} order by `${columes[0]}`"
        def select_res = sql "select `${columes[0]}` from ${table_name} order by `${columes[0]}`"
        logger.info("after stream load ${table_name}, select result: ${select_res}".toString())

        //check data in table and check table schema
        def select_res_now = "true"
        for(i = 0; i < 5; i++){
            //select_res_now = sql "select * from ${table_name} order by `${columes[0]}`"
            select_res_now = sql "select `${columes[0]}` from ${table_name} order by `${columes[0]}`"
            //logger.info("after alter schema, it's ${i} time select,  select result: ${select_res}".toString())
            assertEquals(select_res, select_res_now)
            sleep(3000)
        }
    }

    def timeout = 180000
    def delta_time = 10000
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

    def index_res = ""
    def create_index = { table_name, colume_name, index_name, index_type, expect_success ->
        // create index
        try{
            real_res = "success"
            if(index_type == "null"){
                index_res = sql """
                create index ${index_name} on ${table_name}(`${colume_name}`) using inverted
                """
            }else{
                index_res = sql """
                create index ${index_name} on ${table_name}(`${colume_name}`) using inverted PROPERTIES("parser"="${index_type}")
                """
            }
            logger.info("create index res: ${index_res} \n".toString())
            wait_for_latest_op_on_table_finish(table_name, timeout)

        }catch(Exception ex){
            logger.info("create create index ${index_name} on ${table_name}(`${colume_name}`) using inverted(${index_type}) fail, catch exception: ${ex} \n".toString())
            real_res = "false"
        }finally{
            assertEquals(expect_success, real_res)
            if(expect_success == "false"){
                logger.info("${table_name} expect fail")
                return
            }
        }
    }

    def drop_index = { table_name, colume_name, index_name, expect_success ->
        // create index
        try{
            sql """
                drop index ${index_name} on ${table_name};
            """
            wait_for_latest_op_on_table_finish(table_name, timeout)

        }catch(Exception ex){
            logger.info("drop index ${index_name} on ${table_name}, catch exception: ${ex} \n".toString())
            real_res = "false"
        }finally{
            assertEquals(expect_success, real_res)
            if(expect_success == "false"){
                logger.info("${table_name} expect fail")
                return
            }
        }
    }

    //start test
    String[] data_models = ["DUPLICATE"]
    int[] replica_num = [1]
    def expect_success = "true"
    def feishu_fix_columes = ["id", "content.post.zh_cn.title", "msg_type"]
    def feishu_fix_columes_type = ["BIGINT", "VARCHAR(100)", "CHAR(50)"]
    def github_fix_columes = ["repo.id"]
    def github_fix_columes_type = ["BIGINT"]
    def table_name = ["feishu", "github"]
    ArrayList<String> table_names = new ArrayList<>()
    //step1: create table
    for (def j=0; j < data_models.size(); j++){
        if( data_models[j] == "AGGREGATE" ){
            expect_success = "false"
        }

        for(def k=0; k < replica_num.size(); k++){
            // expect create table
            for(def t=0; t< table_name.size(); t++){
                if(table_name[t] == "feishu"){
                    table_names.add("dytable_complex_feishu_${replica_num[k]}_${data_models[j]}")
                    test_create_and_load_dynamic_table("dytable_complex_feishu_${replica_num[k]}_${data_models[j]}", data_models[j], replica_num[k], feishu_fix_columes, feishu_fix_columes_type, "dynamic_feishu_alarm.json", expect_success)
                } else if(table_name[t] == "github"){
                    table_names.add("dytable_complex_github_${replica_num[k]}_${data_models[j]}")
                    test_create_and_load_dynamic_table("dytable_complex_github_${replica_num[k]}_${data_models[j]}", data_models[j], replica_num[k], github_fix_columes, github_fix_columes_type, "dynamic_github_events.json", expect_success)
                }
            }
        }
    }
    // expect create table false
    test_create_and_load_dynamic_table("test_dytable_complex_data_feishu_array_key_colume", "DUPLICATE", 3, ["content.post.zh_cn.content.tag"], ["ARRAY<ARRAY<text>>"], "dynamic_feishu_alarm.json", "false")
    logger.info("recording tables: ${table_names}".toString())


    def test_round = 3
    for(def tb=0; tb < table_names.size(); tb++){
        for(def round = 0; round < test_round; round++){
            if((round % test_round) == 1){
                if(table_names[tb].contains("feishu")) {
                    create_index("${table_names[tb]}", "content.post.zh_cn.title", "title_idx", "english", "success")
                    create_index("${table_names[tb]}", "msg_type", "msg_idx", "null", "success")
                    //select index colume mix select
                    qt_sql """ select * from ${table_names[tb]} where msg_type="post" or `content.post.zh_cn.title` match_all "BUILD_FINISHED" order by `content.post.zh_cn.title`, id limit 30; """
                    qt_sql """ select * from ${table_names[tb]} where msg_type in ("post", "get") and `content.post.zh_cn.title` match_any "BUILD_FINISHED" order by `content.post.zh_cn.title`, id limit 30; """
                    qt_sql """ select `content.post.zh_cn.content.herf` from ${table_names[tb]} where msg_type in ("post") and `content.post.zh_cn.title` match_any "FINISHED" order by `content.post.zh_cn.title`,id limit 30; """
                    qt_sql """ select count() from ${table_names[tb]} where msg_type="post" and `content.post.zh_cn.title` != "BUILD_FINISHED" group by `content.post.zh_cn.title`; """
                    // qt_sql """ select `content.post.zh_cn.title`,`content.post.zh_cn.content.herf` from dytable_complex_feishu_3_DUPLICATE where msg_type="post" group by `content.post.zh_cn.content.herf`, `content.post.zh_cn.title` order by `content.post.zh_cn.title`;"""
                }else if(table_names[tb].contains("github")) {
                    create_index("${table_names[tb]}", "actor.id", "actorid_idx", "null", "success")
                    create_index("${table_names[tb]}", "payload.pull_request.title", "title_idx", "english", "success")
                    // index colume select
                    //qt_sql """ select * from ${table_names[tb]} where `actor.id`=93110249 or `payload.pull_request.title`="" order by `actor.id` limit 100; """
                    qt_sql """select `repo.name` from ${table_names[tb]} where `actor.id`=93110249 or `payload.pull_request.title`="" order by `actor.id`; """
                    // index colume and  simple colume mix select
                    //qt_sql """ select * from ${table_names[tb]} where `actor.id`!= 93110249 order by `actor.id` limit 100;"""
                    qt_sql """select `repo.name`, type from ${table_names[tb]} where `actor.id`!= 93110249 order by `actor.id`, `repo.name` limit 100;"""
                    qt_sql """ select * from ${table_names[tb]} where `actor.id`!= 93110249 order by `actor.id`, `repo.name`, type limit 10;"""
                    // index colume and common array colume mix select
                    qt_sql """ select `repo.name`, count() from ${table_names[tb]} where `payload.pull_request.title`="" and `repo.id`=444318240 GROUP BY `repo.name` order by `repo.name`;"""
                    qt_sql """ select `repo.name`, count() from ${table_names[tb]} where `actor.id` != 93110249 GROUP BY `repo.name` order by `repo.name`;"""

            }else if((round % test_round) == 2){
                if(table_names[tb].contains("feishu")) {
                    drop_index("${table_names[tb]}", "content.post.zh_cn.title", "title_idx", "success")
                    drop_index("${table_names[tb]}", "msg_type", "msg_idx", "success")
                }else if(table_names[tb].contains("github")) {
                    drop_index("${table_names[tb]}", "actor.id", "actorid_idx", "success")
                    drop_index("${table_names[tb]}", "payload.pull_request.title", "title_idx", "success")
                }
            }else{
                if(table_names[tb].contains("feishu")) {
                    qt_sql """ select count() from ${table_names[tb]} WHERE msg_type="post" group by msg_type; """
                    qt_sql """ select * from ${table_names[tb]} WHERE msg_type="post" and content.post.zh_cn.title="BUILD_FINISHED" order by `content.post.zh_cn.title` limit 50;; """
                    qt_sql """ select count() from ${table_names[tb]} where msg_type="post" and `content.post.zh_cn.title` != "BUILD_FINISHED" group by `content.post.zh_cn.title`; """
                    // qt_sql """ select `content.post.zh_cn.content.herf` from where msg_type="post" group by `content.post.zh_cn.content.herf` order by `content.post.zh_cn.title`;"""
                }else if(table_names[tb].contains("github")) {
                    qt_sql """ SELECT count() FROM ${table_names[tb]}  WHERE type = 'WatchEvent' GROUP BY payload.action;"""
                    qt_sql """ SELECT `repo.name`, count() AS stars FROM ${table_names[tb]} WHERE type = 'WatchEvent' AND year(created_at) = '2015' GROUP BY repo.name ORDER BY `repo.name`, `actor.id` DESC LIMIT 50;"""
                    qt_sql """ SELECT element_at(`payload.commits.author.email`, 1) from ${table_names[tb]} WHERE size(`payload.commits.author.email`) > 0 and size(`payload.commits.author.email`) <= 3 order by `actor.id`; """
                    }
                }
            }
        }
    }
}
