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


suite("test_index_match_phrase_select", "inverted_index_select"){
    def indexTbName1 = "test_index_match_phrase_select"
    def varchar_colume1 = "name"
    def varchar_colume2 = "grade"
    def varchar_colume3 = "fatherName"
    def varchar_colume4 = "matherName"
    def char_colume1 = "studentInfo"
    def string_colume1 = "tearchComment"
    def text_colume1 = "selfComment"
    def date_colume1 = "registDate"
    def int_colume1 = "age"

    def timeout = 120000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
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

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    // create table with different index
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                ${varchar_colume1} varchar(50),
                ${varchar_colume2} varchar(30) NOT NULL,
                ${varchar_colume3} varchar(50),
                ${varchar_colume4} varchar(50),
                otherinfo varchar(100),
                ${int_colume1} int NOT NULL,
                ${date_colume1} datetime NULL,
                ${char_colume1} char(100),
                ${string_colume1} string,
                ${text_colume1} text,
                INDEX ${varchar_colume1}_idx(${varchar_colume1}) USING INVERTED PROPERTIES("parser"="english") COMMENT '${varchar_colume1} index',
                INDEX ${int_colume1}_idx(${int_colume1}) USING INVERTED COMMENT '${int_colume1} index',
                INDEX ${varchar_colume2}_idx(${varchar_colume2}) USING INVERTED PROPERTIES("parser"="none") COMMENT '${varchar_colume2} index',
                INDEX ${string_colume1}_idx(${string_colume1}) USING INVERTED PROPERTIES("parser"="english") COMMENT '${string_colume1} index',
                INDEX ${char_colume1}_idx(${char_colume1}) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${char_colume1} index',
                INDEX ${text_colume1}_idx(${text_colume1}) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${text_colume1} index',
                INDEX ${varchar_colume3}_idx(${varchar_colume3}) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' ${varchar_colume3} index'
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """

    // insert data
    // ${varchar_colume1}, ${varchar_colume2}, ${varchar_colume3}, ${varchar_colume4}, otherinfo, ${int_colume1}, ${date_colume1}, ${char_colume1}, ${string_colume1}, ${text_colume1}
    // name, grade, fatherName, motherName, otherinfo, age, registDate, studentInfo, tearchComment, selfComment
    sql """ insert into ${indexTbName1} VALUES
        ("zhang san", "grade 5", "zhang yi", "chen san", "buy dancing book", 10, "2017-10-01", "tall:120cm, weight: 35kg, hobbies: sing, dancing", "Like cultural and recreational activities", "Class activists"),
        ("zhang san yi", "grade 5", "zhang yi", "chen san", "buy", 11, "2017-10-01", "tall:120cm, weight: 35kg, hobbies: reading book", "A quiet little boy", "learn makes me happy"),
        ("li si", "grade 4", "li er", "wan jiu", "", 9, "2018-10-01", "tall:100cm, weight: 30kg, hobbies: playing ball", "A naughty boy", "i just want go outside"),
        ("san zhang", "grade 5", "", "", "", 10, "2017-10-01", "tall:100cm, weight: 30kg, hobbies:", "", ""),
        ("li sisi", "grade 6", "li ba", "li liuliu", "", 11, "2016-10-01", "tall:150cm, weight: 40kg, hobbies: sing, dancing, running", "good at handiwork and beaty", "")
    """
    
    for (int i = 0; i < 2; i++) {
        logger.info("select table with index times " + i)
        // case1: match phrase with index but support_phrase=fasle
        if (i > 0) {
            // case2: match phrase with index and support_phrase=true
            logger.info("it's " + i + " times select, not first select, drop all index before select again")
            sql """
                ALTER TABLE ${indexTbName1}
                    drop index ${varchar_colume1}_idx,
                    drop index ${varchar_colume2}_idx,
                    drop index ${varchar_colume3}_idx,
                    drop index ${int_colume1}_idx,
                    drop index ${string_colume1}_idx,
                    drop index ${char_colume1}_idx,
                    drop index ${text_colume1}_idx;
            """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)

            // readd index
            logger.info("it's " + i + " times select, readd all index before select again")
            sql """
                ALTER TABLE ${indexTbName1}
                    add index ${varchar_colume1}_idx(`${varchar_colume1}`) USING INVERTED PROPERTIES("parser"="english", "support_phrase" = "true") COMMENT '${varchar_colume1} index',
                    add index ${varchar_colume2}_idx(`${varchar_colume2}`) USING INVERTED PROPERTIES("parser"="none", "support_phrase" = "true") COMMENT '${varchar_colume2} index',
                    add index ${varchar_colume3}_idx(`${varchar_colume3}`) USING INVERTED PROPERTIES("parser"="standard", "support_phrase" = "true") COMMENT ' ${varchar_colume3} index',
                    add index ${int_colume1}_idx(`${int_colume1}`) USING INVERTED COMMENT '${int_colume1} index',
                    add index ${string_colume1}_idx(`${string_colume1}`) USING INVERTED PROPERTIES("parser"="english", "support_phrase" = "true") COMMENT '${string_colume1} index',
                    add index ${char_colume1}_idx(`${char_colume1}`) USING INVERTED PROPERTIES("parser"="standard", "support_phrase" = "true") COMMENT '${char_colume1} index',
                    add index ${text_colume1}_idx(`${text_colume1}`) USING INVERTED PROPERTIES("parser"="standard", "support_phrase" = "true") COMMENT '${text_colume1} index';
            """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            if (!isCloudMode()) {
                sql """ build index ${varchar_colume1}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${varchar_colume2}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${varchar_colume3}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${int_colume1}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${string_colume1}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${char_colume1}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
                sql """ build index ${text_colume1}_idx on ${indexTbName1} """
                wait_for_build_index_on_partition_finish(indexTbName1, timeout)
            }
        }

        // case1: test match_phrase ""
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume2} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume3} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${string_colume1} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${char_colume1} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${text_colume1} match_phrase "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
        }

        // case2: test int colume cannot use match_phrase
        def colume_match_result = "fail"
        try {
            drop_result = sql "select * from ${indexTbName1} where ${int_colume1} match_phrase 9"
            colume_match_result = 'success'
        } catch(Exception ex) {
            logger.info("int colume should not match succ, result: " + ex)
        }
        assertEquals(colume_match_result, 'fail')

        // case3: test match_phrase varchar column with english parser
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase 'zhang san' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase "li si" order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase '"si li"' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase 'san zhang' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase 'not exist name' order by name; """

        // case4: test match_phrase varchar column with none parser
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}='grade 5' order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}="grade 5" order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}='grade none' order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2} match_phrase 'grade 5' order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2} match_phrase 'grade none' order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume2} match_phrase 'grade' order by name """

        // cas5: test match_phrase varchar column with standard parser
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_phrase 'zhang yi' order by name """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_phrase "li liu" order by name """

        // case6: test match_phrase char column with standard parser
        qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_phrase "tall:100cm" order by name """
        qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_phrase "weight: hobbies:" order by name """
        qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_phrase "hobbies: sing" order by name """

        // case7: test match_phrase string column with english parser
        qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_phrase 'a quiet little boy' order by name """
        qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_phrase "A naughty boy" order by name """
        qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_phrase 'a boy' order by name """

        // case8: test match_phrase text column with standard parser
        qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_phrase 'i just want go outside' order by name """
        qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_phrase "good" order by name """

    }

}