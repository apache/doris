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


suite("test_match_query_without_index", "inverted_index_select"){
    def indexTbName1 = "test_match_query_without_index"
    def varchar_colume1 = "name"
    def varchar_colume2 = "grade"
    def varchar_colume3 = "fatherName"
    def varchar_colume4 = "matherName"
    def char_colume1 = "studentInfo"
    def string_colume1 = "tearchComment"
    def text_colume1 = "selfComment"
    def date_colume1 = "registDate"
    def int_colume1 = "age"

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
                ${text_colume1} text
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
    sql """ set enable_common_expr_pushdown = true """
    // case1: match any
    try {
        sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any "" order by name; """
    } catch(Exception ex) {
        logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
    }

    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'zhang san' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'san' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'not exist name' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'san' or ${varchar_colume3} match_any 'li' order by name """

    // case2: match all
    try {
        sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all "" order by name; """
    } catch(Exception ex) {
        logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_all,  result: " + ex)
    }
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all "zhang san" order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all '"zhang san"' order by name """
    qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_all "tall:100cm, weight: 30kg, hobbies:" order by name """
    qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_all 'A naughty boy' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all "zhang san" or ${string_colume1} match_all 'A naughty boy' order by name """

    // case3: match phrase
    try {
        sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase "" order by name; """
    } catch(Exception ex) {
        logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_phrase,  result: " + ex)
    }
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase 'zhang san' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase "li si" order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase '"si li"' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_phrase "li liu" order by name """
    qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_phrase 'i just want go outside' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_phrase "li liu" or ${string_colume1} match_phrase 'A naughty boy' order by name """

    // case4: match_any, match_all, match_phrase
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_phrase 'zhang san' and ${char_colume1} match_all 'hobbies: reading book' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume4} match_phrase "li liu" and not ${string_colume1} match 'boy' order by name """
    qt_sql """ select * from ${indexTbName1} where not ${string_colume1} match 'boy' order by name """
    qt_sql """ select * from ${indexTbName1} where ${varchar_colume4} match_phrase "li liuliu" and not ${string_colume1} match 'boy' order by name """
}