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

suite("test_string_function_regexp") {
    sql "set batch_size = 4096;"

    def tbName = "test_string_function_regexp"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k varchar(32)
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            ("billie eillish"),
            ("It's ok"),
            ("Emmy eillish"),
            ("It's true")
        """
    // regexp as keyword
    qt_sql "SELECT k FROM ${tbName} WHERE k regexp '^billie' ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k regexp 'ok\$' ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k regexp concat('^', k) order by k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k not regexp '^billie' ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k not regexp 'ok\$' ORDER BY k;"

    // regexp as function
    qt_sql "SELECT k FROM ${tbName} WHERE regexp(k, '^billie') ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE regexp(k, 'ok\$') ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE regexp(k, concat('^', k)) order by k;"
    qt_sql "SELECT k FROM ${tbName} WHERE not regexp(k, '^billie') ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE not regexp(k, 'ok\$') ORDER BY k;"


    // ===== 新增对 regexp_position 的测试 =====

    // —— 基础两参数场景 —— 
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+');"         // 匹配 "123"，期望返回 4
    qt_sql "SELECT regexp_position('no_digits_here', '\\\\d+');"    // 无数字串，期望 -1
    qt_sql "SELECT regexp_position('', '\\\\d+');"                  // 空字符串，期望 -1
    qt_sql "SELECT regexp_position(NULL, '\\\\d+');"                // NULL 输入，期望 -1
    qt_sql "SELECT regexp_position('2025-06-03', '\\\\d+');"        // 匹配 "2025"，期望返回 1

    // —— 三参数场景 (str, pattern, start) —— 
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+', '1');"      // 从第1位开始匹配，匹配 "123"，期望 4
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+', '4');"      // 从第4位开始匹配，"1" 仍在第4位，期望 4
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+', '5');"      // 从第5位开始匹配，匹配 "23"，期望 5
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+', '0');"      // start<1 非法，直接期望 -1
    qt_sql "SELECT regexp_position('abc123def', '\\\\d+', 'xyz');"    // start 不是数字，期望 -1

    // —— 特殊边界情况 —— 
    // 1. 数字串在开头
    qt_sql "SELECT regexp_position('12345', '\\\\d+', '1');"          // "12345" 从第1位匹配，期望 1
    // 2. 整串无匹配
    qt_sql "SELECT regexp_position('abc', '\\\\d+', '1');"            // 无数字串，期望 -1
    // 3. start 超过字符串长度
    qt_sql "SELECT regexp_position('abc123', '\\\\d+', '100');"       // start 超界，期望 -1
    // 4. 空串依然返回 -1
    qt_sql "SELECT regexp_position('', '\\\\d+', '1');"               // 空串，期望 -1
    // 5. Unicode + 数字混合（验证非 ASCII 数字不被 \\d 匹配）
    qt_sql "SELECT regexp_position('中文123', '\\\\d+', '1');"         // "中"(1) "文"(2) "1"(3) 匹配，期望 3
    // 6. 匹配纯文本（非数字）场景
    qt_sql "SELECT regexp_position('HELLO World', 'World', '7');"      // "World" 从第7位开始，期望 7
    // 7. 大小写敏感性
    qt_sql "SELECT regexp_position('HELLO World', 'world', '1');"      // 小写 "world" 不匹配，期望 -1
    // 8. 单字符数字匹配
    qt_sql "SELECT regexp_position('a1b2c3', '\\\\d', '3');"           // 从第3位 'b' 开始，后续数字 "2" 在第4位，期望 4

    // 9. 按单词边界匹配整数字（使用 \\b），并指定 start
    qt_sql "SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', '5');"
    //                                                      ^                  ^        ...期望第一个完整数字 "23" 在第8位，返回 8
    qt_sql "SELECT regexp_position('I have 23 apples, 5 pears and 13 oranges', '\\\\b\\\\d+\\\\b', '12');"
    //                                                      ^                   ^         ...从第12位开始匹配，匹配 "5" 在第19位，返回 19

    // —— 含表测试：批量验证多行 —— 
    def tbPos = "test_regexp_position_tbl"
    sql "DROP TABLE IF EXISTS ${tbPos};"
    sql """
        CREATE TABLE IF NOT EXISTS ${tbPos} (
            id INT,
            txt VARCHAR(50)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql "INSERT INTO ${tbPos} VALUES (1, 'abc123'), (2, 'foo42bar'), (3, 'no_digits'), (4, ''), (5, NULL);"

    // 3.1 两参数场景：输出 (id, pos2)
    qt_sql "SELECT id, regexp_position(txt, '\\\\d+') AS pos2 FROM ${tbPos} ORDER BY id;"

    // 3.2 三参数场景：输出 (id, p1, p3, p100)
    qt_sql """
        SELECT 
            id,
            regexp_position(txt, '\\\\d+', '1')  AS p1,
            regexp_position(txt, '\\\\d+', '3')  AS p3,
            regexp_position(txt, '\\\\d+', '100') AS p100
        FROM ${tbPos}
        ORDER BY id;
    """

    sql "DROP TABLE IF EXISTS ${tbPos};"
    // ===== 新增结束 =====
    qt_sql "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);"
    qt_sql "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 2);"
    qt_sql "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 3);"

    qt_sql "SELECT regexp_extract_or_null('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);"
    qt_sql "SELECT regexp_extract_or_null('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 2);"
    qt_sql "SELECT regexp_extract_or_null('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 3);"

    qt_sql "SELECT regexp_extract_all('x=a3&x=18abc&x=2&y=3&x=4&x=17bcd', 'x=([0-9]+)([a-z]+)');"
    qt_sql "SELECT regexp_extract_all('http://a.m.baidu.com/i41915i73660.htm', 'i([0-9]+)');"
    qt_sql "SELECT regexp_extract_all('abc=111, def=222, ghi=333', '(\"[^\"]+\"|\\\\w+)=(\"[^\"]+\"|\\\\w+)');"
    qt_sql "select regexp_extract_all('xxfs','f');"
    qt_sql "select regexp_extract_all('asdfg', '(z|x|c|)');"
    qt_sql "select regexp_extract_all('abcdfesscca', '(ab|c|)');"
    qt_sql_regexp_extract_all "select regexp_extract_all('', '\"([^\"]+)\":'), length(regexp_extract_all('', '\"([^\"]+)\":')) from test_string_function_regexp;"

    qt_sql "SELECT regexp_replace('a b c', \" \", \"-\");"
    qt_sql "SELECT regexp_replace('a b c','(b)','<\\\\1>');"

    qt_sql "SELECT regexp_replace_one('a b c', \" \", \"-\");"
    qt_sql "SELECT regexp_replace_one('a b b','(b)','<\\\\1>');"

    qt_sql_utf1 """ select '皖12345' REGEXP '^[皖][0-9]{5}\$'; """
    qt_sql_utf2 """ select '皖 12345' REGEXP '^[皖] [0-9]{5}\$'; """

    // bug fix
    sql """
        INSERT INTO ${tbName} VALUES
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish"),
            ("billie eillish")
        """
    qt_sql_regexp_null "SELECT /*+SET_VAR(parallel_pipeline_task_num=1)*/regexp_extract(k, cast(null as varchar), 1) from test_string_function_regexp;"
    // end bug fix

    sql "DROP TABLE ${tbName};"

    def tableName= "test"
    sql "use test_query_db"

    //regexp as keyword
    qt_sql "select * from ${tableName} where lower(k7) regexp '.*o4\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) regexp '[yun]+nk' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) regexp '^[a-z]+[0-9]?\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) regexp '^[a-z]+[0-9]+[a-z]+\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) regexp 'wang(juoo|yu)[0-9]+\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) regexp '^[a-o]+[0-9]+[a-z]?\$' order by k1, k2, k3, k4"
    qt_sql "select count(*) from ${tableName} where k1<10 and lower(k6) regexp '^t'"

    //not regexp as keyword
    qt_sql "select * from ${tableName} where lower(k7) not regexp '.*o4\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) not regexp '[yun]+nk' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) not regexp 'wang(juoo|yu)[0-9]+\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) not regexp '^[a-z]+[0-9]?\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) not regexp '^[a-z]+[0-9]+[a-z]+\$' order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where lower(k7) not regexp '^[a-o]+[0-9]+[a-z]?\$' order by k1, k2, k3, k4"
    qt_sql "select count(*) from ${tableName} where k1<10 and lower(k6) not regexp '^t'"

    //regexp as function
    qt_sql "select * from ${tableName} where regexp(lower(k7), '.*o4\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where regexp(lower(k7), '[yun]+nk') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where regexp(lower(k7), '^[a-z]+[0-9]?\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where regexp(lower(k7), '^[a-z]+[0-9]+[a-z]+\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where regexp(lower(k7), 'wang(juoo|yu)[0-9]+\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where regexp(lower(k7), '^[a-o]+[0-9]+[a-z]?\$') order by k1, k2, k3, k4"
    qt_sql "select count(*) from ${tableName} where k1<10 and regexp(lower(k6), '^t')"

    //not regexp as function
    qt_sql "select * from ${tableName} where not regexp(lower(k7), '.*o4\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where not regexp(lower(k7), '[yun]+nk') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where not regexp(lower(k7), 'wang(juoo|yu)[0-9]+\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where not regexp(lower(k7), '^[a-z]+[0-9]?\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where not regexp(lower(k7), '^[a-z]+[0-9]+[a-z]+\$') order by k1, k2, k3, k4"
    qt_sql "select * from ${tableName} where not regexp(lower(k7), '^[a-o]+[0-9]+[a-z]?\$') order by k1, k2, k3, k4"
    qt_sql "select count(*) from ${tableName} where k1<10 and regexp(lower(k6), '^t')"

    def tbName2 = "test_string_function_field"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                id int,
                name varchar(32)
            )
            DISTRIBUTED BY HASH(name) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName2} VALUES 
            (2,"Suzi"),
            (9,"Ben"),
            (7,"Suzi"),
            (8,"Henry"),
            (1,"Ben"),
            (4,"Henry")
        """
    qt_sql_field1 "select name from ${tbName2} order by field(name,'Suzi','Ben','Henry');"
    qt_sql_field2 "select name from ${tbName2} order by field(name,'Ben','Henry');"
    qt_sql_field3 "select name from ${tbName2} order by field(name,'Henry') desc,id;"

    qt_sql_field4 "SELECT FIELD('21','2130', '2131', '21');"
    qt_sql_field5 "SELECT FIELD(21, 2130, 21, 2131);"
}