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

suite("test_string_function", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    qt_sql "select elt(0, \"hello\", \"doris\");"
    qt_sql "select elt(1, \"hello\", \"doris\");"
    qt_sql "select elt(2, \"hello\", \"doris\");"
    qt_sql "select elt(3, \"hello\", \"doris\");"
    qt_sql "select c1, c2, elt(c1, c2) from (select number as c1, 'varchar' as c2 from numbers('number'='5') where number > 0) a;"

    qt_sql "select append_trailing_char_if_absent('a','c');"
    qt_sql "select append_trailing_char_if_absent('ac','c');"

    qt_sql "select ascii('1');"
    qt_sql "select ascii('a');"
    qt_sql "select ascii('A');"
    qt_sql "select ascii('!');"

    qt_sql "select bit_length(\"abc\");"

    qt_sql "select char_length(\"abc\");"

    qt_sql "select concat(\"a\", \"b\");"
    qt_sql "select concat(\"a\", \"b\", \"c\");"
    qt_sql "select concat(\"a\", null, \"c\");"

    qt_sql "select concat_ws(\"or\", \"d\", \"is\");"
    qt_sql "select concat_ws(NULL, \"d\", \"is\");"
    qt_sql "select concat_ws(\"or\", \"d\", NULL,\"is\");"
    qt_sql "select concat_ws(\"or\", [\"d\", \"is\"]);"
    qt_sql "select concat_ws(NULL, [\"d\", \"is\"]);"
    qt_sql "select concat_ws(\"or\", [\"d\", NULL,\"is\"]);"
    qt_sql "select concat_ws(\"or\", [\"d\", \"\",\"is\"]);"

    qt_sql "select ends_with(\"Hello doris\", \"doris\");"
    qt_sql "select ends_with(\"Hello doris\", \"Hello\");"

    qt_sql "select find_in_set(\"b\", \"a,b,c\");"
    qt_sql "select find_in_set(\"d\", \"a,b,c\");"
    qt_sql "select find_in_set(null, \"a,b,c\");"
    qt_sql "select find_in_set(\"a\", null);"

    qt_sql "select hex('1');"
    qt_sql "select hex('12');"
    qt_sql "select hex('@');"
    qt_sql "select hex('A');"
    qt_sql "select hex(12);"
    qt_sql "select hex(-1);"
    qt_sql "select hex('hello,doris')"

    qt_sql "select unhex('@');"
    qt_sql "select unhex('68656C6C6F2C646F726973');"
    qt_sql "select unhex('41');"
    qt_sql "select unhex('4142');"
    qt_sql "select unhex('');"
    qt_sql "select unhex(NULL);"


    qt_sql_unhex_null "select unhex_null('@');"
    qt_sql_unhex_null "select unhex_null('68656C6C6F2C646F726973');"
    qt_sql_unhex_null "select unhex_null('41');"
    qt_sql_unhex_null "select unhex_null('4142');"
    qt_sql_unhex_null "select unhex_null('');"
    qt_sql_unhex_null "select unhex_null(NULL);"

    qt_sql_instr "select instr(\"abc\", \"b\");"
    qt_sql_instr "select instr(\"abc\", \"d\");"
    qt_sql_instr "select instr(\"abc\", null);"
    qt_sql_instr "select instr(null, \"a\");"
    qt_sql_instr "SELECT instr('foobar', '');"
    qt_sql_instr "SELECT instr('上海天津北京杭州', '北京');"

    qt_sql "SELECT lcase(\"AbC123\");"
    qt_sql "SELECT lower(\"AbC123\");"

    qt_sql "SELECT initcap(\"AbC123abc abc.abc,?|abc\");"

    qt_sql "select left(\"Hello doris\",5);"
    qt_sql "select right(\"Hello doris\",5);"

    qt_sql "select length(\"abc\");"

    qt_sql_locate "SELECT LOCATE('bar', 'foobarbar');"
    qt_sql_locate "SELECT LOCATE('xbar', 'foobar');"
    qt_sql_locate "SELECT LOCATE('', 'foobar');"
    qt_sql_locate "SELECT LOCATE('北京', '上海天津北京杭州');"

    qt_sql "SELECT lpad(\"hi\", 5, \"xy\");"
    qt_sql "SELECT lpad(\"hi\", 1, \"xy\");"
    qt_sql "SELECT rpad(\"hi\", 5, \"xy\");"
    qt_sql "SELECT rpad(\"hi\", 1, \"xy\");"

    qt_sql "SELECT ltrim('   ab d');"

    qt_sql "select money_format(17014116);"
    qt_sql "select money_format(1123.456);"
    qt_sql "select money_format(1123.4);"
    qt_sql "select money_format(truncate(1000,10))"

    qt_sql "select null_or_empty(null);"
    qt_sql "select null_or_empty(\"\");"
    qt_sql "select null_or_empty(\"a\");"

    qt_sql "select not_null_or_empty(null);"
    qt_sql "select not_null_or_empty(\"\");"
    qt_sql "select not_null_or_empty(\"a\");"

    qt_sql "SELECT repeat(\"a\", 3);"
    qt_sql "SELECT repeat(\"a\", -1);"
    qt_sql "SELECT repeat(\"a\", 0);"
    qt_sql "SELECT repeat(\"a\",null);"
    qt_sql "SELECT repeat(null,1);"

    qt_sql "select replace(\"https://doris.apache.org:9090\", \":9090\", \"\");"
    qt_sql "select replace(\"https://doris.apache.org:9090\", \"\", \"new_str\");"

    qt_sql "SELECT REVERSE('hello');"

    qt_sql "select split_part('hello world', ' ', 1)"
    qt_sql "select split_part('hello world', ' ', 2)"
    qt_sql "select split_part('hello world', ' ', 0)"
    qt_sql "select split_part('hello world', ' ', -1)"
    qt_sql "select split_part('hello world', ' ', -2)"
    qt_sql "select split_part('hello world', ' ', -3)"
    qt_sql "select split_part('abc##123###xyz', '##', 0)"
    qt_sql "select split_part('abc##123###xyz', '##', 1)"
    qt_sql "select split_part('abc##123###xyz', '##', 3)"
    qt_sql "select split_part('abc##123###xyz', '##', 5)"
    qt_sql "select split_part('abc##123###xyz', '##', -1)"
    qt_sql "select split_part('abc##123###xyz', '##', -2)"
    qt_sql "select split_part('abc##123###xyz', '##', -4)"

    qt_sql "select starts_with(\"hello world\",\"hello\");"
    qt_sql "select starts_with(\"hello world\",\"world\");"
    qt_sql "select starts_with(\"hello world\",null);"

    qt_sql "select strleft(NULL, 1);"
    qt_sql "select strleft(\"good morning\", NULL);"
    qt_sql "select left(NULL, 1);"
    qt_sql "select left(\"good morning\", NULL);"
    qt_sql "select strleft(\"Hello doris\", 5);"
    qt_sql "select left(\"Hello doris\", 5)"
    qt_sql "select strright(NULL, 1);"
    qt_sql "select strright(\"good morning\", NULL);"
    qt_sql "select right(NULL, 1);"
    qt_sql "select right(\"good morning\", NULL);"
    qt_sql "select strright(\"Hello doris\", 5);"
    qt_sql "select right(\"Hello doris\", 5);"
    qt_sql "select strleft(\"good morning\", 120);"
    qt_sql "select strleft(\"good morning\", -5);"
    qt_sql "select strright(\"Hello doris\", 120);"
    qt_sql "select strright(\"Hello doris\", -5);"
    qt_sql "select left(\"good morning\", 120);"
    qt_sql "select left(\"good morning\", -5);"
    qt_sql "select right(\"Hello doris\", 120);"
    qt_sql "select right(\"Hello doris\", -6);"

    qt_convert_1 "select convert('装装装装装' using gbk);"

    sql """ drop table if exists left_right_test; """
    sql """ create table left_right_test (
        id INT NULL,
        name VARCHAR(16) NULL
    )
    UNIQUE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1");
    """
    sql """
        insert into left_right_test values
        (1, "Isaac Newton"),
        (2, "Albert Einstein"),
        (3, "Marie Curie"),
        (4, "Charles Darwin"),
        (5, "Stephen Hawking");
    """

    qt_select_null_str """
    select
    id,
    strleft(name, 5),
    strright(name, 5),
    left(name, 6),
    right(name, 6)
    from left_right_test
    order by id;
    """

    sql """ drop table if exists left_right_test; """
    sql """ create table left_right_test (
        id INT,
        name VARCHAR(16)
    )
    UNIQUE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num"="1");
    """
    sql """
        insert into left_right_test values
        (1, "Isaac Newton"),
        (2, "Albert Einstein"),
        (3, "Marie Curie"),
        (4, "Charles Darwin"),
        (5, "Stephen Hawking");
    """

    qt_select_not_null_str """
    select
    id,
    strleft(name, 5),
    strright(name, 5),
    left(name, 6),
    right(name, 6)
    from left_right_test
    order by id;
    """

    qt_sql "select substring('abc1', 2);"
    qt_sql "select substring('abc1', -2);"
    qt_sql "select substring('abc1', 5);"
    qt_sql "select substring('abc1def', 2, 2);"
    qt_sql "select substring('abcdef',3,-1);"
    qt_sql "select substring('abcdef',-3,-1);"
    qt_sql "select substring('abcdef',10,1);"
    sql """ set debug_skip_fold_constant = true;"""
    qt_substring_utf8_sql "select substring('中文测试',5);"
    qt_substring_utf8_sql "select substring('中文测试',4);"
    qt_substring_utf8_sql "select substring('中文测试',2,2);"
    qt_substring_utf8_sql "select substring('中文测试',-1,2);"
    sql """ set debug_skip_fold_constant = false;"""
    qt_substring_utf8_sql "select substring('中文测试',5);"
    qt_substring_utf8_sql "select substring('中文测试',4);"
    qt_substring_utf8_sql "select substring('中文测试',2,2);"
    qt_substring_utf8_sql "select substring('中文测试',-1,2);"

    sql """ drop table if exists test_string_function; """
    sql """ create table test_string_function (
        k1 varchar(16),
        v1 int
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
    sql """ insert into test_string_function values
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1),
        ("aaaaaaaa", 1)
    """
    // bug fix
    qt_sql_substring1 """ select /*+SET_VAR(parallel_pipeline_task_num=1)*/ substring(k1, cast(null as int), cast(null as int)) from test_string_function; """

    qt_sql "select substr('a',3,1);"
    qt_sql "select substr('a',2,1);"
    qt_sql "select substr('a',1,1);"
    qt_sql "select substr('a',0,1);"
    qt_sql "select substr('a',-1,1);"
    qt_sql "select substr('a',-2,1);"
    qt_sql "select substr('a',-3,1);"
    qt_sql "select substr('abcdef',3,-1);"
    qt_sql "select substr('abcdef',-3,-1);"

    qt_mid_1 "select mid('a',0,1);"
    qt_mid_2 "select mid('a',-1,1);"
    qt_mid_3 "select mid('a',1,1);"
    qt_mid_4 "select mid('a',-2,1);"
    qt_mid_5 "select mid('a',2,1);"
    qt_mid_6 "select mid('a',-3,1);"
    qt_mid_7 "select mid('a',3,1);"
    qt_mid_8 "select mid('abcdef',-3,-1);"
    qt_mid_9 "select mid('abcdef',3,-1);"
    qt_mid_10 "select mid('',3,-1);"
    qt_mid_11 "select mid('abcdef',3,10);"
    qt_mid_12 "select mid('abcdef',-3);"
    qt_mid_13 "select mid('abcdef',3);"
    qt_mid_14 "select mid('',3);"
    qt_mid_15 "select mid('a' FROM 0 FOR 1);"
    qt_mid_16 "select mid('a' FROM -1 FOR 1);"
    qt_mid_17 "select mid('a' FROM 1 FOR 1);"
    qt_mid_18 "select mid('a' FROM -2 FOR 1);"
    qt_mid_19 "select mid('a' FROM 2 FOR 1);"
    qt_mid_20 "select mid('a' FROM -3 FOR 1);"
    qt_mid_21 "select mid('a' FROM 3 FOR 1);"
    qt_mid_22 "select mid('abcdef' FROM -3 FOR -1);"
    qt_mid_23 "select mid('abcdef' FROM 3 FOR -1);"
    qt_mid_24 "select mid('' FROM 3 FOR -1);"
    qt_mid_25 "select mid('abcdef' FROM 3 FOR 10);"
    qt_mid_26 "select mid('abcdef' FROM -3);"
    qt_mid_27 "select mid('abcdef' FROM 3);"
    qt_mid_28 "select mid('' FROM 3);"
    qt_mid_29 "select mid(NULL, 2);"
    qt_mid_30 "select mid(NULL, 2, 3)"
    qt_mid_31 "select mid(NULL FROM 2);"
    qt_mid_32 "select mid(NULL FROM 2 FOR 3);"
    qt_mid_33 "select mid('hello', NULL);"
    qt_mid_34 "select mid('hello', 2, NULL);"

    qt_sql "select sub_replace(\"this is origin str\",\"NEW-STR\",1);"
    qt_sql "select sub_replace(\"doris\",\"***\",1,2);"

    qt_sql "select substring_index(\"hello world\", \" \", 1);"
    qt_sql "select substring_index(\"hello world\", \" \", 2);"
    qt_sql "select substring_index(\"hello world\", \" \", 3);"
    qt_sql "select substring_index(\"hello world\", \" \", -1);"
    qt_sql "select substring_index(\"hello world\", \" \", -2);"
    qt_sql "select substring_index(\"hello world\", \" \", -3);"
    qt_sql "select substring_index(\"prefix__string2\", \"__\", 2);"
    qt_sql "select substring_index(\"prefix__string2\", \"_\", 2);"
    qt_sql "select substring_index(\"prefix_string2\", \"__\", 1);"
    qt_sql "select substring_index(null, \"__\", 1);"
    qt_sql "select substring_index(\"prefix_string\", null, 1);"
    qt_sql "select substring_index(\"prefix_string\", \"_\", null);"
    qt_sql "select substring_index(\"prefix_string\", \"__\", -1);"

    qt_sql "select elt(0, \"hello\", \"doris\");"
    qt_sql "select elt(1, \"hello\", \"doris\");"
    qt_sql "select elt(2, \"hello\", \"doris\");"
    qt_sql "select elt(3, \"hello\", \"doris\");"

    qt_sql "select sub_replace(\"this is origin str\",\"NEW-STR\",1);"
    qt_sql "select sub_replace(\"doris\",\"***\",1,2);"

    // test function char
    test {
        sql """ select char(68 using abc); """
        exception "only support charset name 'utf8'"
    }

    // const
    qt_sql_func_char_const1 """ select char(68); """
    qt_sql_func_char_const2 """ select char(68, 111, 114, 105, 115); """
    qt_sql_func_char_const3 """ select char(0, 68, 111, 114, 105, 115); """
    qt_sql_func_char_const4 """ select char(68, 111, 114, 105, 115, 0); """
    qt_sql_func_char_const5 """ select length(char(68, 111, 114, 105, 115, 0)); """
    qt_sql_func_char_const6 """ select char(68, 111, 114, 0, 105, null, 115 using utf8); """
    qt_sql_func_char_const7 """ select char(229, 164, 154); """
    qt_sql_func_char_const8 """ select length(char(229, 164, 154 using utf8)); """
    qt_sql_func_char_const9 """ select char(15049882, 15179199, 14989469); """

    sql "drop table if exists test_function_char;";
    sql """ create table test_function_char (
        k1 tinyint not null,
        k2 smallint,
        k3 int,
        k4 bigint
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """

    sql """ insert into test_function_char values
        (97, 98, 99, 100),
        (97, null, 99, 100),
        (97, 98, null, 100),
        (97, 98, 99, null),
        (0, 98, 99, 100),
        (97, 0, 99, 100),
        (97, 98, 0, 100),
        (97, 98, 99, 0)
    """
    qt_sql_func_char1 """ select char(k1) from test_function_char order by k1; """
    qt_sql_func_char2 """ select char(k1, k2, k3, k4) from test_function_char order by k1, k2, k3, k4; """
    qt_sql_func_char3 """ select char(65, k1, k2, k3, k4) from test_function_char order by k1, k2, k3, k4; """
    qt_sql_func_char4 """ select char(k1, 15049882, k2, k3, k4) from test_function_char order by k1, k2, k3, k4; """
    qt_sql_func_char5 """ select char(k1, k2, k3, k4, 15049882) from test_function_char order by k1, k2, k3, k4; """

    sql "drop table if exists test_function_char;";
    sql """ create table test_function_char (
        k1 int not null,
        k2 int,
        k3 int,
        k4 int
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """

    sql """ insert into test_function_char values
        (229, 164, 154, 0),
        (15049882, null, 15179199, 14989469),
        (15049882, 15179199, null, 14989469),
        (15049882, 15179199, 14989469, null)
    """
    qt_sql_func_char6 """ select char(k1) from test_function_char order by k1; """
    qt_sql_func_char7 """ select char(k1, k2, k3, k4) from test_function_char order by k1, k2, k3, k4; """
    qt_sql_func_char8 """ select char(k1, k2, k3, k4, 65) from test_function_char order by k1, k2, k3, k4; """
    qt_sql_func_char9 """ select char(0) = ' '; """
    qt_sql_func_char10 """ select char(0) = '\0'; """

    qt_strcmp1 """ select strcmp('a', 'abc'); """
    qt_strcmp2 """ select strcmp('abc', 'abc'); """
    qt_strcmp3 """ select strcmp('abcd', 'abc'); """

    sql "drop table if exists test_function_ngram_search;";
    sql """ create table test_function_ngram_search (
        k1 int not null,
        s string null 
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """

    sql """  insert into test_function_ngram_search values(1,"fffhhhkkkk"),(2,"abc1313131"),(3,'1313131') ,(4,'abc') , (5,null)"""

    qt_ngram_search1 """ select k1, ngram_search(s,'abc1313131',3) as x , s from test_function_ngram_search order by x ;"""

    qt_ngram_search2 """select ngram_search('abc','abc1313131',3); """
    qt_ngram_search3 """select ngram_search('abc1313131','abc1313131',3); """
    qt_ngram_search3 """select ngram_search('1313131','abc1313131',3); """
    

    sql "drop table if exists test_function_ngram_search;";
    sql """ create table test_function_ngram_search (
        k1 int not null,
        s string not null 
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """

    sql """  insert into test_function_ngram_search values(1,"fffhhhkkkk"),(2,"abc1313131"),(3,'1313131') ,(4,'abc') """

    qt_ngram_search1_not_null """ select k1, ngram_search(s,'abc1313131',3) as x , s from test_function_ngram_search order by x ;"""

    sql "SELECT random_bytes(7);"
    qt_sql_random_bytes "SELECT random_bytes(null);"
    test {
        sql " select random_bytes(-1); "
        exception "argument -1 of function random_bytes at row 0 was invalid"
    }
    def some_result = sql """ SELECT random_bytes(10) a FROM numbers("number" = "10") """
    assertTrue(some_result[0][0] != some_result[1][0], "${some_result[0][0]} should different with ${some_result[1][0]}")
    sql "select random_bytes(k1) from test_function_ngram_search;"

    explain {
        sql("""select/*+SET_VAR(enable_fold_constant_by_be=true)*/ random_bytes(10) from numbers("number" = "10");""")
        contains "final projections: random_bytes(10)"
    }
    explain {
        sql("""select/*+SET_VAR(enable_fold_constant_by_be=true)*/ random(10) from numbers("number" = "10");""")
        contains "final projections: random(10)"
    }

    sql "DROP TABLE IF EXISTS test_make_set;"
    sql"""CREATE TABLE test_make_set (
        id int,
        bit_num BIGINT,
        vc1 VARCHAR(50),
        vc2 VARCHAR(50),
        vc3 VARCHAR(50)
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ( 'replication_num' = '1' );"""

    sql"""INSERT INTO test_make_set (id, bit_num, vc1, vc2, vc3) VALUES
    (1, 1, 'apple', 'orange', NULL),
    (2, 2, 'red', 'blue', NULL),
    (3, 3, 'dog', 'cat', 'bird'),
    (4, 4, 'small', 'medium', 'large'),
    (5, 5, 'hot', 'warm', NULL),
    (6, 6, 'monday', 'tuesday', 'wednesday'),
    (7, 7, 'one', 'two', 'three'),
    (8, 0, 'hello', 'world', NULL),
    (9, -2, 'test1', 'test2', 'test3'),
    (10, -3, '汽车', '自行车', '火车'),
    (11, NULL, 'a', 'b', 'c'),
    (12, 7, NULL, NULL, NULL),
    (13, 3, '', 'should after ,', 'useless'),
    (14, BIT_SHIFT_LEFT(1, 50) - 3, 'first', 'second', 'third');"""

    qt_mask_set_1"""SELECT MAKE_SET(bit_num, vc1, vc2, vc3) FROM test_make_set;"""
    qt_mask_set_2"""SELECT MAKE_SET(id, vc1, vc2, vc3) FROM test_make_set;"""
    qt_mask_set_3"""SELECT MAKE_SET(BIT_SHIFT_LEFT(1, 63) + BIT_SHIFT_LEFT(1, 62) + BIT_SHIFT_LEFT(1, 61) + BIT_SHIFT_LEFT(1, 50) + BIT_SHIFT_LEFT(1, 25) + BIT_SHIFT_LEFT(1, 3) + BIT_SHIFT_LEFT(1, 1), 'x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20','x21','x22','x23','x24','x25','x26','x27','x28','x29','x30','x31','x32','x33','x34','x35','x36','x37','x38','x39','x40','x41','x42','x43','x44','x45','x46','x47','x48','x49','x50','x51','x52','x53','x54','x55','x56','x57','x58','x59','x60','x61','x62','x63','x64','x65','x66','x67','x68','x69','x70');"""
    qt_mask_set_4"""SELECT MAKE_SET(BIT_SHIFT_LEFT(1, 62) + BIT_SHIFT_LEFT(1, 60) + BIT_SHIFT_LEFT(1, 58) + BIT_SHIFT_LEFT(1, 45) + BIT_SHIFT_LEFT(1, 5) + BIT_SHIFT_LEFT(1, 2), 'y1', NULL, '', 'y4','y5','y6','y7','y8','y9','y10', 'y11','y12','y13','y14','y15','y16','y17','y18','y19','y20', 'y21','y22','y23','y24','y25','y26','y27','y28','y29','y30', 'y31','y32','y33','y34','y35','y36','y37','y38','y39','y40', 'y41','y42','y43','y44','y45',NULL,'y47','y48');"""
    qt_mask_set_5"""SELECT id, MAKE_SET(bit_num, 'const1', vc2, 'const3') FROM test_make_set ORDER BY id;"""
    qt_mask_set_6"""SELECT id, MAKE_SET(3, vc1, 'middle', vc3) FROM test_make_set ORDER BY id;"""
    qt_mask_set_7"""SELECT id, MAKE_SET(bit_num, 'Doris', 'Apache', vc3) FROM test_make_set ORDER BY id;"""
    qt_mask_set_8"""SELECT id, MAKE_SET(id, vc1, NULL, 'constant') FROM test_make_set ORDER BY id;"""
    qt_mask_set_9"""SELECT id, MAKE_SET(7, vc1, vc2, 'third') FROM test_make_set ORDER BY id;"""
    qt_mask_set_10"""SELECT id, MAKE_SET(bit_num, '第一', '第二', vc3) FROM test_make_set ORDER BY id;"""
    qt_mask_set_11"""SELECT id, MAKE_SET(5, 'alpha', vc2, 'gamma') FROM test_make_set ORDER BY id;"""
    qt_mask_set_12"""SELECT id, MAKE_SET(bit_num, vc1, 'fixed', NULL) FROM test_make_set ORDER BY id;"""

    testFoldConst("SELECT MAKE_SET(1, 'Doris', 'Apache', 'Database');")
    testFoldConst("SELECT MAKE_SET(2, 'hello', 'goodbye', 'world');")
    testFoldConst("SELECT MAKE_SET(3, NULL, '你好', '世界');")
    testFoldConst("SELECT MAKE_SET(-2, 'a', 'b', 'c');")
    testFoldConst("SELECT MAKE_SET(NULL, 'a', 'b', 'c');")
    testFoldConst("SELECT MAKE_SET(4, 'a', 'b', NULL);")
    testFoldConst("SELECT MAKE_SET(4611686018427387903, 'a', 'b', 'c');")
    testFoldConst("SELECT MAKE_SET(BIT_SHIFT_LEFT(1, 50) - 3, 'first', 'second', 'third');")
    testFoldConst("SELECT MAKE_SET(3, '', 'a');")

    test {
        sql"""SELECT MAKE_SET(184467440737095516156, 'a', 'b', 'c');"""
        exception "Can not find the compatibility function signature"
    }

    // EXPORT_SET
    sql """DROP TABLE IF EXISTS test_export_set;"""
    sql """CREATE TABLE `test_export_set` (
            `id` INT,
            `bits` BIGINT,
            `on` VARCHAR(255),
            `off` VARCHAR(255),
            `sep` VARCHAR(255),
            `num_of_b` INT
        )DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ( 'replication_num' = '1' );"""
    sql """INSERT INTO `test_export_set` VALUES
            (1, -1, '1', '0', ',', 50),
            (2, -2, '1', '0', '', 64),
            (3, 5, 'Y', 'N', ',', 5),
            (4, 5, '1', '0', '', 64),
            (5, 5, '', '0', '', 65),
            (6, 6, '1', '', '', 63),
            (7, 19284249819, '1', '0', ',', 64),
            (8, 9, 'apache', 'doris', '|123|', 64),
            (9, NULL, '1', '0', ',', 5),
            (10, 5, NULL, '0', '', 5),
            (11, 5, '1', NULL, ',', 10),
            (12, 5, '1', '0', NULL, 10),
            (13, 5, '1', '0', ',', NULL);"""

    qt_export_set_1 """SELECT id, EXPORT_SET(`bits`, `on`, `off`, `sep`, `num_of_b`) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_2 """SELECT EXPORT_SET(7, '1', '0');"""
    qt_export_set_3 """SELECT EXPORT_SET(7, '你好', '0', '?');"""
    qt_export_set_4 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 64), '1', '0');"""
    qt_export_set_5 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63) - 1, '1', '0');"""
    qt_export_set_6 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63), '1', '0');"""
    qt_export_set_7 """SELECT EXPORT_SET(-BIT_SHIFT_LEFT(1, 63), '1', '0');"""
    qt_export_set_8 """SELECT EXPORT_SET(-1, '1', '0');"""
    qt_export_set_9 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 64) - 1, '1', '0');"""
    qt_export_set_10 """SELECT EXPORT_SET(99999999999999999999, '1', '0');"""
    qt_export_set_11 """SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 1) + 1, '1', '0');"""
    qt_export_set_12 """SELECT EXPORT_SET(0, '1', '0');"""
    qt_export_set_13 """SELECT EXPORT_SET(1, '1', '0');"""
    qt_export_set_14 """SELECT EXPORT_SET(-0, '1', '0');"""
    qt_export_set_15 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 62), '1', '0');"""
    qt_export_set_16 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 62) + BIT_SHIFT_LEFT(1, 63), '1', '0');"""
    qt_export_set_17 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 60), '1', '0');"""
    qt_export_set_18 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63) - 1, '1', '0', ',', 32);"""
    qt_export_set_19 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63), '1', '0', ',', 128);"""
    qt_export_set_20 """SELECT EXPORT_SET(-1, '1', '0', ',', -5);"""
    qt_export_set_21 """SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 60), '1', '0', ',', 0);"""
    qt_export_set_22 """SELECT EXPORT_SET(255, '1', '0', ',', 8);"""
    qt_export_set_23 """SELECT EXPORT_SET(1023, '1', '0', ',', 10);"""
    qt_export_set_24 """SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 2) + 1, '1', '0');"""
    qt_export_set_25 """SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 1) * 2, '1', '0');"""
    qt_export_set_26 """SELECT EXPORT_SET(18446744073709551616, '1', '0');""" // 2^64
    qt_export_set_27 """SELECT EXPORT_SET(18446744073709551615, '1', '0');""" // 2^64 -1
    qt_export_set_28 """SELECT EXPORT_SET(9223372036854775808, '1', '0');""" // 2^63
    qt_export_set_29 """SELECT EXPORT_SET(1180591620717411303424, '1' ,'0');""" // 2^70
    qt_export_set_30 """SELECT EXPORT_SET(18446744073708551616, '1', '0');"""
    qt_export_set_31 """SELECT EXPORT_SET(-9223372036854775808, '1', '0');"""
    qt_export_set_32 """SELECT EXPORT_SET(-9223372036854775809, '1', '0');"""
    qt_export_set_33 """SELECT EXPORT_SET(-9223372036854775807, '1', '0');"""
    qt_export_set_34 """SELECT id, EXPORT_SET(`bits`, `on`, `off`, ' ! ', `num_of_b`) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_35 """SELECT id, EXPORT_SET(`bits`, `on`, `off`, '|分隔符|', '17') FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_36 """SELECT id, EXPORT_SET(5, `on`, '0', '#', 5) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_36 """SELECT id, EXPORT_SET(`bits`, `on`, `off`) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_37 """SELECT id, EXPORT_SET(-7, `on`, `off`) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_38 """SELECT id, EXPORT_SET(114514, '1', '0', `sep`) FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_39 """SELECT id, EXPORT_SET(`bits`, `on`, '0', '世界!?你好')FROM `test_export_set` ORDER BY `id`;"""
    qt_export_set_40 """SELECT id, EXPORT_SET(`bits`, '1', '0', ',', 5) FROM `test_export_set` ORDER BY `id`;"""
    testFoldConst("SELECT EXPORT_SET(7, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(7, '你好', '0', '?');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 64), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63) - 1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-BIT_SHIFT_LEFT(1, 63), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 64) - 1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(99999999999999999999, '1', '0');")
    testFoldConst("SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 1) + 1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(0, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-0, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 62), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 62) + BIT_SHIFT_LEFT(1, 63), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 60), '1', '0');")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63) - 1, '1', '0', ',', 32);")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 63), '1', '0', ',', 128);")
    testFoldConst("SELECT EXPORT_SET(-1, '1', '0', ',', -5);")
    testFoldConst("SELECT EXPORT_SET(BIT_SHIFT_LEFT(1, 60), '1', '0', ',', 0);")
    testFoldConst("SELECT EXPORT_SET(255, '1', '0', ',', 8);")
    testFoldConst("SELECT EXPORT_SET(1023, '1', '0', ',', 10);")
    testFoldConst("SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 2) + 1, '1', '0');")
    testFoldConst("SELECT EXPORT_SET((BIT_SHIFT_LEFT(1, 63) - 1) * 2, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(18446744073709551616, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(18446744073709551615, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(9223372036854775808, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(1180591620717411303424, '1' ,'0');")
    testFoldConst("SELECT EXPORT_SET(18446744073708551616, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-9223372036854775808, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-9223372036854775809, '1', '0');")
    testFoldConst("SELECT EXPORT_SET(-9223372036854775807, '1', '0');")

    // INSERT function tests
    sql """DROP TABLE IF EXISTS test_insert_function;"""
    sql """CREATE TABLE `test_insert_function` (
            `id` INT,
            `str` VARCHAR(255),
            `pos` BIGINT,
            `len` BIGINT,
            `newstr` VARCHAR(255)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ( 'replication_num' = '1' );"""
    
    sql """INSERT INTO `test_insert_function` VALUES
            (1, 'Quadratic', 3, 4, 'What'),
            (2, 'Quadratic', -1, 4, 'What'),
            (3, 'Quadratic', 3, 100, 'What'),
            (4, 'Quadratic', 0, 4, 'What'),
            (5, 'Quadratic', 100, 4, 'What'),
            (6, 'Quadratic', 3, 0, 'What'),
            (7, 'Quadratic', 3, -1, 'What'),
            (8, 'Hello World', 7, 5, 'Doris'),
            (9, 'Hello World', 1, 5, 'Hi'),
            (10, 'Hello World', 12, 1, '!'),
            (11, '', 1, 0, 'test'),
            (12, 'test', 1, 0, ''),
            (13, 'test', 1, 10, ''),
            (14, '你好世界', 3, 2, 'Apache'),
            (15, 'Hello', 3, 2, '你好'),
            (16, '你好世界', 2, 1, 'Doris'),
            (17, 'abcdefg', 1, 7, 'xyz'),
            (18, 'abcdefg', 4, 0, 'xyz'),
            (19, 'abcdefg', 8, 1, 'xyz'),
            (20, 'test', 2, 2, ''),
            (21, '测试字符串', 1, 2, '新'),
            (22, '测试字符串', 3, 10, '内容'),
            (23, 'Special!@#', 8, 3, '***'),
            (24, 'a', 1, 1, 'b'),
            (25, 'a', 2, 0, 'b'),
            (26, NULL, 1, 1, 'test'),
            (27, 'test', NULL, 1, 'new'),
            (28, 'test', 1, NULL, 'new'),
            (29, 'test', 1, 1, NULL),
            (30, '🎉🎊🎈', 2, 1, '🎁');"""

    qt_insert_1 """SELECT id, INSERT(`str`, `pos`, `len`, `newstr`) FROM `test_insert_function` ORDER BY `id`;"""
    qt_insert_2 """SELECT id, INSERT('Quadratic', `pos`, 4, 'What') FROM `test_insert_function` ORDER BY `id`;"""
    qt_insert_3 """SELECT id, INSERT(`str`, 3, `len`, 'What') FROM `test_insert_function` ORDER BY `id`;"""
    qt_insert_4 """SELECT id, INSERT(`str`, `pos`, 2, `newstr`) FROM `test_insert_function` ORDER BY `id`;"""
    testFoldConst("SELECT INSERT('Quadratic', 3, 4, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', -1, 4, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 3, 100, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 0, 4, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 100, 4, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 3, 0, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 3, -1, 'What');")
    testFoldConst("SELECT INSERT('Quadratic', 3, -100, 'What');")
    testFoldConst("SELECT INSERT('Hello World', 7, 5, 'Doris');")
    testFoldConst("SELECT INSERT('Hello World', 1, 5, 'Hi');")
    testFoldConst("SELECT INSERT('Hello World', 12, 1, '!');")
    testFoldConst("SELECT INSERT('', 1, 0, 'test');")
    testFoldConst("SELECT INSERT('test', 1, 0, '');")
    testFoldConst("SELECT INSERT('test', 1, 10, '');")
    testFoldConst("SELECT INSERT('test', 1, 100, '');")
    testFoldConst("SELECT INSERT('你好世界', 3, 2, 'Apache');")
    testFoldConst("SELECT INSERT('Hello', 3, 2, '你好');")
    testFoldConst("SELECT INSERT('你好世界', 2, 1, 'Doris');")
    testFoldConst("SELECT INSERT('你好世界', 1, 4, 'Test');")
    testFoldConst("SELECT INSERT('你好世界', 5, 1, '!');")
    testFoldConst("SELECT INSERT('abcdefg', 1, 7, 'xyz');")
    testFoldConst("SELECT INSERT('abcdefg', 4, 0, 'xyz');")
    testFoldConst("SELECT INSERT('abcdefg', 8, 1, 'xyz');")
    testFoldConst("SELECT INSERT('abcdefg', 1, 3, '123');")
    testFoldConst("SELECT INSERT('abcdefg', 4, 3, '456');")
    testFoldConst("SELECT INSERT('abcdefg', 7, 1, '!');")
    testFoldConst("SELECT INSERT('test', 2, 2, '');")
    testFoldConst("SELECT INSERT('test', 1, 0, 'prefix');")
    testFoldConst("SELECT INSERT('test', 5, 0, 'suffix');")
    testFoldConst("SELECT INSERT('测试字符串', 1, 2, '新');")
    testFoldConst("SELECT INSERT('测试字符串', 3, 10, '内容');")
    testFoldConst("SELECT INSERT('测试字符串', 1, 5, 'Apache Doris');")
    testFoldConst("SELECT INSERT('Special!@#', 8, 3, '***');")
    testFoldConst("SELECT INSERT('Special!@#', 1, 7, 'Normal');")
    testFoldConst("SELECT INSERT('a', 1, 1, 'b');")
    testFoldConst("SELECT INSERT('a', 2, 0, 'b');")
    testFoldConst("SELECT INSERT('a', 1, 0, 'b');")
    testFoldConst("SELECT INSERT('a', 1, 2, 'b');")
    testFoldConst("SELECT INSERT('🎉🎊🎈', 2, 1, '🎁');")
    testFoldConst("SELECT INSERT('🎉🎊🎈', 1, 3, '🎁');")
    testFoldConst("SELECT INSERT('abc', 2, 1, 'XYZ');")
    testFoldConst("SELECT INSERT('abc', 2, 2, 'XYZ');")
    testFoldConst("SELECT INSERT('abc', 1, 1, 'XYZ');")
    testFoldConst("SELECT INSERT('abc', 3, 1, 'XYZ');")
    testFoldConst("SELECT INSERT('abcdefghijklmnopqrstuvwxyz', 10, 8, '12345');")
    testFoldConst("SELECT INSERT('abcdefghijklmnopqrstuvwxyz', 1, 26, 'ALPHABET');")
    testFoldConst("SELECT INSERT('abcdefghijklmnopqrstuvwxyz', 14, 13, 'END');")
    testFoldConst("SELECT INSERT('test', 2, 2, 'EST');")
    testFoldConst("SELECT INSERT('test', 1, 4, 'TEST');")
    testFoldConst("SELECT INSERT('test', 3, 2, 'ST');")
    testFoldConst("SELECT INSERT('0123456789', 1, 10, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 5, 1, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 11, 0, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 0, 5, 'X');")
    testFoldConst("SELECT INSERT('0123456789', -5, 5, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 5, 0, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 5, -1, 'X');")
    testFoldConst("SELECT INSERT('0123456789', 5, 100, 'X');")
    testFoldConst("SELECT INSERT('Hello World!', 7, 6, 'Doris!');")
    testFoldConst("SELECT INSERT('Apache Doris', 8, 5, 'Database');")
    testFoldConst("SELECT INSERT(NULL, 1, 1, 'test');")
    testFoldConst("SELECT INSERT('test', NULL, 1, 'new');")
    testFoldConst("SELECT INSERT('test', 1, NULL, 'new');")
    testFoldConst("SELECT INSERT('test', 1, 1, NULL);")
}
