suite("smoke_test_index_match", "smoke"){
    if (context.config.cloudVersion != null && !context.config.cloudVersion.isEmpty()
            && compareCloudVersion(context.config.cloudVersion, "3.0.0") >= 0) {
        log.info("case: smoke_test_index_match, cloud version ${context.config.cloudVersion} bigger than 3.0.0, skip".toString());
        return
    }
    def indexTbName1 = "index_range_match_term_and_phrase_select"
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
                 break
            }
            useTime = t
            sleep(delta_time)
        }
        sleep(delta_time)
        assertTrue(useTime <= OpTimeout)
    }

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    // create table with different index
    /*
    sql """
            CREATE TABLE IF NOT EXISTS ${indexTbName1} (
                name varchar(50),
                age int NOT NULL,
                registDate datetime NULL,
                studentInfo char(100),
                tearchComment string,
                selfComment text,
                grade varchar(30) NOT NULL,
                fatherName varchar(50),
                matherName varchar(50),
                otherinfo varchar(100),
                INDEX name_idx(name) USING INVERTED COMMENT 'name index',
                INDEX age_idx(age) USING INVERTED COMMENT 'age index',
                INDEX grade_idx(grade) USING INVERTED PROPERTIES("parser"="none") COMMENT 'grade index',
                INDEX tearchComment_index(tearchComment) USING INVERTED PROPERTIES("parser"="english") COMMENT 'tearchComment index',
                INDEX studentInfo_index(studentInfo) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX selfComment_index(selfComment) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'studentInfo index',
                INDEX fatherName_idx(fatherName) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' fatherName index'
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10;
        """
    sql """ insert into ${indexTbName1} VALUES
            ("zhang san", 10, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: sing, dancing", "Like cultural and recreational activities", "Class activists", "zhang yi", "chen san", "buy dancing book"),
            ("zhang san yi", 11, "grade 5", "2017-10-01", "tall:120cm, weight: 35kg, hobbies: reading book", "A quiet little boy", "learn makes me happy", "zhang yi", "chen san", "buy"),
            ("li si", 9, "grade 4", "2018-10-01",  "tall:100cm, weight: 30kg, hobbies: playing ball", "A naughty boy", "i just want go outside", "li er", "wan jiu", ""),
            ("san zhang", 10, "grade 5", "2017-10-01", "tall:100cm, weight: 30kg, hobbies:", "", "", "", "", ""),
            ("li sisi", 11, "grade 6", "2016-10-01", "tall:150cm, weight: 40kg, hobbies: sing, dancing, running", "good at handiwork and beaty", "", "li ba", "li liuliu", "")
    """
    */

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
            DISTRIBUTED BY HASH(`name`) BUCKETS 10;
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
        // case 1
        if (i > 0) {
            logger.info("it's " + i + " times select, not first select, drop all index before select again")
            sql """ drop index ${varchar_colume1}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${varchar_colume2}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${varchar_colume3}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${int_colume1}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${string_colume1}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${char_colume1}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ drop index ${text_colume1}_idx on ${indexTbName1} """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)

            // readd index
            logger.info("it's " + i + " times select, readd all index before select again")
            sql """ create index ${varchar_colume1}_idx on ${indexTbName1}(`${varchar_colume1}`) USING INVERTED PROPERTIES("parser"="english") COMMENT '${varchar_colume1} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${varchar_colume2}_idx on ${indexTbName1}(`${varchar_colume2}`) USING INVERTED PROPERTIES("parser"="none") COMMENT '${varchar_colume2} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${varchar_colume3}_idx on ${indexTbName1}(`${varchar_colume3}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT ' ${varchar_colume3} index'"""
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${int_colume1}_idx on ${indexTbName1}(`${int_colume1}`) USING INVERTED COMMENT '${int_colume1} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${string_colume1}_idx on ${indexTbName1}(`${string_colume1}`) USING INVERTED PROPERTIES("parser"="english") COMMENT '${string_colume1} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${char_colume1}_idx on ${indexTbName1}(`${char_colume1}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${char_colume1} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
            sql """ create index ${text_colume1}_idx on ${indexTbName1}(`${text_colume1}`) USING INVERTED PROPERTIES("parser"="standard") COMMENT '${text_colume1} index' """
            wait_for_latest_op_on_table_finish(indexTbName1, timeout)
        }

        // case1: match term
        // case1.0 test match ""
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume2} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${varchar_colume3} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${string_colume1} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${char_colume1} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }
        try {
            sql """ select * from ${indexTbName1} where ${text_colume1} match_any "" order by name; """
        } catch(Exception ex) {
            logger.info("select * from ${indexTbName1} where ${varchar_colume1} match_any,  result: " + ex)
        }

        // case1.0 test int colume cannot use match
        def colume_match_result = "fail"
        try {
            drop_result = sql "select * from ${indexTbName1} where ${int_colume1} match_any 9"
            colume_match_result = 'success'
        } catch(Exception ex) {
            logger.info("int colume should not match succ, result: " + ex)
        }
        assertEquals(colume_match_result, 'fail')

        // case2: match same term with different way
        // case2.0： test varchar default(simple) match same term with different way
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'zhang san' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all "zhang san" order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_all '"zhang san"' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'san' order by name; """
        qt_sql """ select * from ${indexTbName1} where ${varchar_colume1} match_any 'not exist name' order by name; """

        // case2.1: test varchar none match same term with different way and repeate 5 times
        for (int test_times = 0; test_times < 5; test_times++) {
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}='grade 5' order by name """
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}="grade 5" order by name """
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume2}='grade none' order by name """
        }

        // cas2.2 test varchar standard match same term with different way and repeate 5 times
        for (test_times = 0; test_times < 5; test_times++) {
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_any 'zhang yi' order by name """
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_all "zhang yi" order by name """
            qt_sql """ select * from ${indexTbName1} where ${varchar_colume3} match_any '"zhang yi"' order by name """
        }

        // case3: test char standard match same term with different way and repeate 5 times
        for (test_times = 0; test_times < 5; test_times++) {
            qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_any 'tall:100cm, weight: 30kg, hobbies:' order by name """
            qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_all "tall:100cm, weight: 30kg, hobbies:" order by name """
            qt_sql """ select * from ${indexTbName1} where ${char_colume1} match_any '"tall:100cm, weight: 30kg, hobbies:"' order by name """
        }

        // case4: test string simple match same term with different way and repeate 5 times
        for (test_times = 0; test_times < 5; test_times++) {
            qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_all 'A naughty boy' order by name """
            qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_any "A naughty boy" order by name """
            qt_sql """ select * from ${indexTbName1} where ${string_colume1} match_any '"A naughty boy"' order by name """
        }

        // case5: test text standard match same term with different way and repeate 5 times
        for (test_times = 0; test_times < 5; test_times++) {
            qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_all 'i just want go outside' order by name """
            qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_any "i just want go outside" order by name """
            qt_sql """ select * from ${indexTbName1} where ${text_colume1} match_all '"i just want go outside"' order by name """
        }

        // case6: test term and phrase mix select
        qt_sql """
            select * from ${indexTbName1} where
                ${varchar_colume1} match_any 'zhang san' and 
                ${varchar_colume2} = 'grade 5' or
                ${varchar_colume3} match_all "zhang yi"
                order by name
            """

        qt_sql """
            select * from ${indexTbName1} where
                ${char_colume1} match_all "tall:100cm, weight: 30kg, hobbies:" and
                ${string_colume1} match_any "A naughty boy" or
                ${text_colume1} match_all "i just want go outside"
                order by name
            """

    }

}
