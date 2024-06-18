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
    qt_sql_substring1 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/ substring(k1, cast(null as int), cast(null as int)) from test_string_function; """

    qt_sql "select substr('a',3,1);"
    qt_sql "select substr('a',2,1);"
    qt_sql "select substr('a',1,1);"
    qt_sql "select substr('a',0,1);"
    qt_sql "select substr('a',-1,1);"
    qt_sql "select substr('a',-2,1);"
    qt_sql "select substr('a',-3,1);"
    qt_sql "select substr('abcdef',3,-1);"
    qt_sql "select substr('abcdef',-3,-1);"

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
}
