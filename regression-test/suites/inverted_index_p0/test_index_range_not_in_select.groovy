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


suite("test_index_range_not_in_select", "inverted_index_select"){
    def varchar_colume1 = "name"
    def varchar_colume2 = "grade"
    def varchar_colume3 = "fatherName"
    def varchar_colume4 = "matherName"
    def char_colume1 = "studentInfo"
    def string_colume1 = "tearchComment"
    def text_colume1 = "selfComment"
    def date_colume1 = "registDate"
    def int_colume1 = "age"
    def key = ""

    sql "set enable_vectorized_engine=true"

    def test_index_table_not_in_with_different_data_model = { Tb_name, data_model, key_columes ->

        //generate key set
        key = ""
        for (def col=0; col<key_columes.size(); col++){
            key += "`${key_columes[col]}`"
            if(col < key_columes.size() - 1){
                key += ", "
            }
        }

        sql """
                CREATE TABLE IF NOT EXISTS ${Tb_name} (
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
                INDEX ${varchar_colume1}_idx(${varchar_colume1}) USING INVERTED COMMENT '${varchar_colume1} index',
                INDEX ${int_colume1}_idx(${int_colume1}) USING INVERTED COMMENT '${int_colume1} index',
                INDEX ${varchar_colume2}_idx(${varchar_colume2}) USING INVERTED PROPERTIES("parser"="none") COMMENT '${varchar_colume2} index',
                INDEX ${string_colume1}_idx(${string_colume1}) USING INVERTED PROPERTIES("parser"="english") COMMENT '${string_colume1} index',
                INDEX ${char_colume1}_idx(${char_colume1}) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${char_colume1} index',
                INDEX ${text_colume1}_idx(${text_colume1}) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${text_colume1} index',
                INDEX ${varchar_colume3}_idx(${varchar_colume3}) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' ${varchar_colume3} index'
            )
            ${data_model} KEY(${key})
            DISTRIBUTED BY HASH(`${key_columes[0]}`) BUCKETS 10
            properties("replication_num" = "1");
            """

        sql """ insert into ${Tb_name} VALUES
        ("zhang san", "grade 5", "zhang yi", "chen san", "buy dancing book", 10, "2017-10-01", "tall:120cm, weight: 35kg, hobbies: sing, dancing", "Like cultural and recreational activities", "Class activists"),
        ("zhang san yi", "grade 5", "zhang yi", "chen san", "buy", 11, "2017-10-01", "tall:120cm, weight: 35kg, hobbies: reading book", "A quiet little boy", "learn makes me happy"),
        ("li si", "grade 4", "li er", "wan jiu", "", 9, "2018-10-01", "tall:100cm, weight: 30kg, hobbies: playing ball", "A naughty boy", "i just want go outside"),
        ("san zhang", "grade 5", "", "", "", 10, "2017-10-01", "tall:100cm, weight: 30kg, hobbies:", "", ""),
        ("li sisi", "grade 6", "li ba", "li liuliu", "", 11, "2016-10-01", "tall:150cm, weight: 40kg, hobbies: sing, dancing, running", "good at handiwork and beaty", "")
        """

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

        for (int i = 0; i < 2; i++) {
            logger.info("select table with index times " + i)
            // case 1
            if (i > 0) {
                logger.info("it's " + i + " times select, not first select, drop all index before select again")
                sql """ drop index ${varchar_colume1}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${varchar_colume2}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${varchar_colume3}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${int_colume1}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${string_colume1}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${char_colume1}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ drop index ${text_colume1}_idx on ${Tb_name} """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)

                // readd index
                logger.info("it's " + i + " times select, readd all index before select again")
                sql """ create index ${varchar_colume1}_idx on ${Tb_name}(`${varchar_colume1}`) USING INVERTED COMMENT '${varchar_colume1} index'"""
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${varchar_colume2}_idx on ${Tb_name}(`${varchar_colume2}`) USING INVERTED PROPERTIES("parser"="none") COMMENT '${varchar_colume2} index'"""
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${varchar_colume3}_idx on ${Tb_name}(`${varchar_colume3}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' ${varchar_colume3} index'"""
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${int_colume1}_idx on ${Tb_name}(`${int_colume1}`) USING INVERTED COMMENT '${int_colume1} index' """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${string_colume1}_idx on ${Tb_name}(`${string_colume1}`) USING INVERTED PROPERTIES("parser"="english") COMMENT '${string_colume1} index' """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${char_colume1}_idx on ${Tb_name}(`${char_colume1}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${char_colume1} index' """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
                sql """ create index ${text_colume1}_idx on ${Tb_name}(`${text_colume1}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${text_colume1} index' """
                wait_for_latest_op_on_table_finish(Tb_name, timeout)
            }

            // case1: select in
            // case1.0: select not not in specific condition
            qt_sql """ select * from ${Tb_name} where ${int_colume1} not in (-1) order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume1} not in ("") order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume2} not in ("") order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume3} not in ("") order by name"""
            qt_sql """ select * from ${Tb_name} where ${string_colume1} not in ("") order by name"""
            qt_sql """ select * from ${Tb_name} where ${char_colume1} not in ("") order by name"""
            qt_sql """ select * from ${Tb_name} where ${text_colume1} not in ("") order by name"""

            // case1.1: select not in single condtion
            qt_sql """ select * from ${Tb_name} where ${int_colume1} not in (10) order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume1} not in ("zhang san yi") order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume2} not in ('grade 5') order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume3} not in ("li liuliu") order by name"""
            qt_sql """ select * from ${Tb_name} where ${string_colume1} not in ("good at handiwork and beaty") order by name"""
            qt_sql """ select * from ${Tb_name} where ${char_colume1} not in ("tall:150cm, weight: 40kg, hobbies: sing, dancing, running") order by name"""
            qt_sql """ select * from ${Tb_name} where ${text_colume1} not in ("learn makes me happy") order by name"""
            // case1.1: select not in muti condtion
            qt_sql """ select * from ${Tb_name} where ${int_colume1} not in (10, 11) order by name """
            qt_sql """ select * from ${Tb_name} where ${varchar_colume1} not in ("li sisi", "zhang san yi") order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume2} not in ("grade 5", 'grade 6') order by name"""
            qt_sql """ select * from ${Tb_name} where ${varchar_colume3} not in ("li liuliu", "zhang yi") order by name"""
            qt_sql """ select * from ${Tb_name} where ${string_colume1} not in ("A quiet little boy", "good at handiwork and beaty") order by name"""
            qt_sql """ select * from ${Tb_name} where ${char_colume1} not in ("tall:150cm, weight: 40kg, hobbies: sing, dancing, running",  "tall:100cm, weight: 30kg, hobbies:") order by name"""
            qt_sql """ select * from ${Tb_name} where ${text_colume1} not in ("i just want go outside", "learn makes me happy") order by name"""

            // case2: select muti index volume
            // case2.0: int index colume and default inverted colume mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${int_colume1} not in (10, 11) and
                    ${varchar_colume1} not in ("li sisi", "zhang san yi")
                order by name
                """
            // case2.1: int index colume and none inverted colume mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${int_colume1} not in (10, 11) and
                    ${varchar_colume2} not in ('grade 5','grade 6')
                order by name
                """
            // case2.2: int index colume and standard inverted colume(different type) mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${int_colume1} not in (10, 11) and
                    ${varchar_colume3} not in ("li liuliu", "zhang yi")
                order by name
                """
            qt_sql """
                select * from ${Tb_name}
                where
                    ${int_colume1} not in (10, 11) and
                    ${string_colume1} not in ("A quiet little boy", "good at handiwork and beaty")
                order by name
                """
            qt_sql """
                select * from ${Tb_name}
                where
                    ${int_colume1} not in (10, 11) and
                    ${char_colume1} not in ("tall:150cm, weight: 40kg, hobbies: sing, dancing, running",  "tall:100cm, weight: 30kg, hobbies:")
                order by name
                """

            // case2.3: string inverted colume mix select
            // case2.3.0: standard inverted colume(different type) mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${varchar_colume3} not in ("li liuliu", "zhang yi") and
                    ${text_colume1} not in ("i just want go outside", "learn makes me happy") or
                    ${char_colume1} not in ("tall:150cm, weight: 40kg, hobbies: sing, dancing, running",  "tall:100cm, weight: 30kg, hobbies:")
                order by name
                """
            // case2.3.0: none, simple and standard colume mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${varchar_colume2} not in ('grade 5','grade 6') or
                    ${string_colume1} not in ("A quiet little boy", "good at handiwork and beaty") and
                    ${varchar_colume3} not in ("li liuliu", "zhang yi")
                order by name
                """
            // case2.4 inverted comlume and common colume mix select
            // case2.4.0: common colume and default inverted colume mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${date_colume1} not in ("2017-10-01", "2018-10-01") or
                    ${varchar_colume1} not in ("li sisi", "zhang san yi")
                order by name
                """
            // case2.1: common colume and none inverted colume mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${date_colume1} not in ("2017-10-01", "2018-10-01") or
                    ${varchar_colume2} not in ('grade 5','grade 6')
                order by name
                """
            // case2.2: common columeand standard inverted colume(different type) mix select
            qt_sql """
                select * from ${Tb_name}
                where
                    ${date_colume1} not in ("2017-10-01", "2018-10-01") or
                    ${varchar_colume3} not in ("li liuliu", "zhang yi")
                order by name
                """
            qt_sql """
                select * from ${Tb_name}
                where
                    ${date_colume1} not in ("2017-10-01", "2018-10-01") or
                    ${string_colume1} not in ("A quiet little boy", "good at handiwork and beaty")
                order by name
                """
            qt_sql """
                select * from ${Tb_name}
                where
                    ${date_colume1} not in ("2017-10-01", "2018-10-01") or
                    ${char_colume1} not in ("tall:150cm, weight: 40kg, hobbies: sing, dancing, running",  "tall:100cm, weight: 30kg, hobbies:")
                order by name
                """
        }
    }

    //test duplicate
    def indexTbName1="duplicate_index_range_not_in_select"
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    test_index_table_not_in_with_different_data_model(indexTbName1, "DUPLICATE", ["${varchar_colume1}", "${varchar_colume2}"])
}