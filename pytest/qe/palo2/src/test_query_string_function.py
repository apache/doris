#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys

sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger

LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()


def test_query_string_ascii():
    """
    {
    "title": "test_query_string_function.test_query_string_ascii",
    "describe": "test for string function ascii",
    "tag": "function,p0"
    }
    """
    """
    test for string function ascii
    """
    line = "select ascii(k7) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ascii(k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ascii('北京,love')"
    runner.check(line)


def test_query_string_find_in_set():
    """
    {
    "title": "test_query_string_function.test_query_string_find_in_set",
    "describe": "test for string function find_in_set",
    "tag": "function,p0"
    }
    """
    """
    test for string function find_in_set"
    """
    line = "select find_in_set(k7,\"wangjuoo4,wangynnsf,lalala,yunlj8\") from %s\
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select find_in_set(k7,\" \") from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select find_in_set('背景', '背景'), find_in_set('详细设计', '设计'), \
            find_in_set('', '详细'), find_in_set('desgine设计', 'desgine设计')"
    runner.check(line)


def test_query_string_instr():
    """
    {
    "title": "test_query_string_function.test_query_string_instr",
    "describe": "test for string function instr",
    "tag": "function,p0"
    }
    """
    """
    test for string function instr
    """
    line = "select instr(lower(k6), \"e\") from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select instr(lower(k7), \"g\") from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select instr(k7, \" \") from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select instr(lower('meaningful 具有重大影响'), '大')"
    runner.check(line)


def test_query_string_length():
    """
    {
    "title": "test_query_string_length",
    "describe": "test for string function length",
    "tag": "function,p0"
    }
    """
    """
    test for string function length
    """
    line = "select length(k7), length(k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select length('meaningful 具有重大影响')"
    runner.check(line)


def test_query_string_locate():
    """
    {
    "title": "test_query_string_function.test_query_string_locate",
    "describe": "test for string function locate",
    "tag": "function,p0"
    }
    """
    """
    test for string function locate
    """
    line = "select locate(\"g\", lower(k7)) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select locate(\"n\", lower(k7), 8) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select locate('影响', 'meaningful具有重大影响')"
    runner.check(line)


def test_query_string_pad():
    """
    {
    "title": "test_query_string_function.test_query_string_pad",
    "describe": "test for string function lpad,rpad",
    "tag": "function,p0"
    }
    """
    """
    test for string function lpad,rpad
    """
    line = "select lpad(k6,20,k7) from %s where k7<>\"\" and k7<>\" \" order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select lpad(k7,20,k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select rpad(k7,20,k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select rpad(k6,20,k7) from %s where k7<>\"\" and k7<>\" \" order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select lpad('哈哈', 30,'呵呵'), rpad('哈哈', 30,'呵呵'), \
            lpad('哈哈', 30, 'hh'), rpad('hh', 30,'呵呵')"
    runner.check(line)


def test_query_string_reverse():
    """
    {
    "title": "test_query_string_function.test_query_string_reverse",
    "describe": "test for string function reverse",
    "tag": "function,p0"
    }
    """
    """
    test for string function reverse
    """
    line = "select reverse(k7) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select reverse(k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select reverse('我有一头小毛驴lala')"
    runner.check(line)


def test_query_string_substr():
    """
    {
    "title": "test_query_string_function.test_query_string_substr",
    "describe": "test for string function substr",
    "tag": "function,p0"
    }
    """
    """
    test for string function substr
    """
    line = "select substr(k7, 2, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k7, 2, 5) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k7, 2, 50) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k7, 20, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k6, 2, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k6, 2, 5) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k6, 2, 50) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substr(k6, 20, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k7, 2, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k7, 2, 5) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k7, 2, 50) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k7, 20, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k6, 2, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k6, 2, 5) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k6, 2, 50) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring(k6, 20, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select substring('lalala我有一头小毛驴', 2, 15)"


def test_query_string_trim():
    """
    {
    "title": "test_query_string_function.test_query_string_trim",
    "describe": "test for string function trim",
    "tag": "function,p0"
    }
    """
    """
    test for string function trim
    """
    line = "select ltrim(concat(\"  \", k6, k7, \"  \")) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select rtrim(concat(\"  \", k6, k7, \"  \")) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select trim(\"  wjj  \"), trim('  北京  ')"
    runner.check(line)
    line = "select trim(concat(\"  \", k6, k7, \"  \")) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)


def test_query_string_repeat():
    """
    {
    "title": "test_query_string_function.test_query_string_repeat",
    "describe": "test for string function repeat",
    "tag": "function,p0"
    }
    """
    """
    test for string function repeat
    """
    line = "select repeat(k6, 4), repeat(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = 'select repeat("哈哈", 5)'
    runner.check(line)


def test_string_strleftright():
    """
    {
    "title": "test_query_string_function.test_string_strleftright",
    "describe": "test for string function strleft, strright",
    "tag": "function,p0"
    }
    """
    """
    test for string function strleft, strright
    """
    line1 = "select strleft(k6, 0) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k6, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strleft(k6, 10) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k6, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strleft(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strleft(k7, 0) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k7, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strleft(k7, 100) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k7, 100) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strleft(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select left(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k6, 0) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k6, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k6, 10) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k6, 10) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k6, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k7, 0) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k7, 0) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k7, 100) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k7, 100) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select right(k7, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line1, line2)
    line1 = "select strright('我有一头小毛驴lala', 7), strleft('我有一头小毛驴lala', 7)"
    line2 = "select right('我有一头小毛驴lala', 7), left('我有一头小毛驴lala', 7)"


def test_query_upper_lower_ucase_lcase():
    """
    {
    "title": "test_query_string_function.test_query_upper_lower_ucase_lcase",
    "describe": "test for function upper, ucase, lcase, lower",
    "tag": "function,p0"
    }
    """
    """
    test for function upper, ucase, lcase, lower
    """
    line = "select upper(k1), upper(k2), upper(k3), upper(k4), upper(k6), upper(k7), \
		    upper(k10), upper(k11) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select ucase(k1), ucase(k2), ucase(k3), ucase(k4), ucase(k6), ucase(k7), \
		    ucase(k10), ucase(k11) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select lcase(DAYNAME(k10)), lcase(DAYNAME(k11)) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select lower(DAYNAME(k10)), lower(DAYNAME(k11)) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select lower('详细设计'), lcase('总体设计'), lcase('‘’、。，')"
    runner.check(line)


def test_query_concat():
    """
    {
    "title": "test_query_string_function.test_query_concat",
    "describe": "test for query concat",
    "tag": "function,p0"
    }
    """
    """
    test for query concat
    """
    line = "select concat(k6, space(10), k7) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat(k6, k7) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat(k7, k6) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat(k6, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat(k7, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    for i in range(1, 5):
        line = "select concat(k%s, k6) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select concat(k%s, k7) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        if (i == 1 or i == 2):
            line = "select concat(space(k%s), k7) from %s order by k1, k2, k3, k4" % (i, table_name)
            runner.check(line)
    # maybe cause Memory exceed limit
    #  else:
    #      line = "select concat(space(k%s), k7) from %s order by k1, k2, k3, k4" % (i, table_name)
    #      runner.checkok(line)
    # there are differences for double and float
    # for i in range(8, 12):
    for i in range(10, 12):
        line = "select concat(k%s, k6) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select concat(k%s, k7) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
    line = "select concat_ws(k6, 'a', 'b', 'c', 'd') from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat_ws(k6, k1, k2, k3, k4, k10, k11) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select concat_ws(k7, 'a', 'b', 'c', 'd') from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select concat_ws(k7, k1, k2, k3, k4, k10, k11) from %s order by k1, k2, k3, k4" \
           % (table_name)
    runner.check(line)
    line = "select concat_ws('，', '测试', '开发', '验收')"
    runner.check(line)


def test_query_ascii():
    """
    {
    "title": "test_query_string_function.test_query_ascii",
    "describe": "test for function ascii",
    "tag": "function,p0"
    }
    """
    """
    test for function ascii
    """
    line = "select ASCII(\"\\0\"), ASCII(\"\\\\\"), ASCII(\"\\b\"), ASCII(\"\\n\"), \
		    ASCII(\"\\r\"), ASCII(\"\\t\"), ASCII(\"\Z\")"
    runner.check(line)
    line = "SELECT ASCII(\"\\a\"), ASCII(\"\X\"), ASCII(\"\z\"), ASCII(\"\?\"), ASCII(\"\*\")"
    runner.check(line)
    line = "SELECT \"\%\", \"\\%\", \"\_\", \"\\_\""
    runner.check(line)
    line = "SELECT \"quote \\\"\", 'quote \\''"
    runner.check(line)
    for i in range(1, 6):
        line = "select ascii(k%s) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)


def test_query_group_concat():
    """
    {
    "title": "test_query_string_function.test_query_group_concat",
    "describe": "group_concat",
    "tag": "function,p0,fuzz"
    }
    """
    """group_concat"""
    line = 'drop view if exists v_concat'
    runner.init(line)
    line = 'create view v_concat as select a.k1, a.k2, b.k6 from baseall a left join bigtable b on \
            a.k1 = b.k1 + 5 order by b.k6, a.k2, a.k1'
    runner.init(line)
    line1 = 'select k2, group_concat(k6) from v_concat group by k2 order by k2'
    line2 = 'select k2, group_concat(k6 order by k6 separator ", ") from v_concat' \
            ' group by k2 order by k2'
    runner.checkok(line1)
    # runner.check2(line1, line2)
    line1 = 'select k2, group_concat(k6, "|") from v_concat group by k2 order by k2'
    line2 = 'select k2, group_concat(k6 order by k6 separator "|") from v_concat' \
            ' group by k2 order by k2'
    runner.checkok(line1)
    # runner.check2(line1, line2)
    line1 = 'select k6, k2, group_concat(cast(k1 as string), ",") from v_concat group by k6, k2' \
            ' order by 2, 3'
    line2 = 'select k6, k2, group_concat(k1 order by k1) from v_concat' \
            ' group by k6, k2 order by 2, 3'
    runner.checkok(line1)
    # runner.check2(line1, line2)
    line = 'select group_concat(distinct k2) from baseall'
    runner.checkwrong(line)
    line = 'select group_concat(k2 order by k2) from baseall'
    runner.checkwrong(line)
    line1 = 'select k2, group_concat(k6, null) from v_concat group by k2 order by k2'
    line2 = 'select k2, group_concat(k6 order by k6 separator ", ") from ' \
            'v_concat group by k2 order by k2'
    runner.checkok(line1)
    # runner.check2(line1, line2)
    line1 = 'select k2, group_concat(null, "|") from v_concat group by k2 order by k2'
    line2 = 'select k2, group_concat(null separator "|") from v_concat group by k2 order by k2'
    runner.checkok(line1)
    # runner.check2(line1, line2)
    line1 = 'select k2, group_concat(null, null) from v_concat group by k2 order by k2'
    line2 = 'select k2, group_concat(null) from v_concat group by k2 order by k2'
    runner.checkok(line1)
    # runner.check2(line1, line2)         


def test_query_get_json_double():
    """
    {
    "title": "test_query_string_function.test_query_get_json_double",
    "describe": "get_json_double(),hive对应的是get_json_object, 精度在11位小数",
    "tag": "function,p0,fuzz"
    }
    """
    """"get_json_double(),hive对应的是get_json_object
        精度在11位小数
    """
    line1 = "SELECT get_json_double('{\"k1\":1.3, \"k2\":\"2\"}', '$.k1')"
    line2 = "SELECT 1.3"
    runner.check2(line1, line2)
    ##精度验证 18位
    line1 = "SELECT get_json_double('{\"k1\":-0.1234567890123456789, \"k2\":\"2\"}', \"$.k1\"), \
        get_json_double('{\"k1\":111, \"k2\":+22.22222222222222}', \"$.k2\"),\
        get_json_double('{\"k1\":111, \"k2\":1234567890.1234567890}', \"$.k2\"),\
        get_json_double('{\"k1\":111, \"k2\":222}', \"$.k2\"),\
        get_json_double('{\"k1\":111111111111.33333333333333, \"k2\":1.1234567890123456789}', \"$.k2\")"
    line2 = "select -0.12345678901234568, null, 1234567890.123457, 222.0, 1.1234567890123457"
    runner.check2(line1, line2)
    ##其他类型的数值
    line1 = " SELECT get_json_double('{\"k1\":1.3, \"k2\":\"2\"}', \"$.k2\"), \
        get_json_double('{\"k1\":1.3, \"k2\":2:888}', \"%.k2\"),\
        get_json_double('{\"k1\":1.3, \"k2\":2:888}', \"$.k3\")"
    line2 = "SELECT NULL, NULL, NULL"
    runner.check2(line1, line2)
    ##json是列表;列表的嵌套不能识别
    line1 = "SELECT get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}', '$.\"my.key\"[1]'),\
           get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}', '$.\"my.key\"[8]'),\
           get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}', '$.\"my.key\"[0]'), \
           get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3], \
             \"my.key\":[1.1, 2.2, 3.3]}',  '$.\"my.key\"[0]'), \
           get_json_double('{\"k1\":\"v1\", \"my.key\":[]}', '$.\"my.key\"[0]')"
    line2 = "SELECT 2.2, null, 1.1, 1.1, null"
    runner.check2(line1, line2)

    line1 = "select get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3, 4.4]}', '$.\"my.key\"[-1]'), \
         get_json_double('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2], \"my.key\":[1.1, 2.2]}', '$.\"my.key\"[0]'), \
         get_json_double('{\"k1\":\"v1\", \"my.key\":[[1.1, 2.2], 3.3, 4.4]}', '$.\"my.key\"[1]'),\
         get_json_double('{\"k1\":\"v1\", \"my.key\":[3.3, [1.1, 2.2], 4.4]}', '$.\"my.key\"[1]')"
    line2 = "SELECT NULL, 1.1, 3.3, NULL"
    runner.check2(line1, line2)

    ##往表里插入数据? 带引号插不进去
    line1 = "select get_json_double('{\"k1\":{\"k1\": [1,2,3]}}', '$.\"k1\".k1.[0]')"
    line2 = "select 1.0"
    runner.check2(line1, line2)
    line1 = "select get_json_double('{\"k1\":1},{\"k1\":2},{\"k2\":2}}', '$.k1')"
    line2 = "select null"
    runner.check2(line1, line2)
    line1 = "select get_json_double('{\"k1\":1},{\"k1\":[2,2]},{\"k2\":2}}', '$.k1')"
    line2 = "select null"
    runner.check2(line1, line2)
    ##checkwrong
    line = "SELECT get_json_double('{\"k1\":1111.1111, \"k2\":2}', '$.k1', '$.k2')"
    runner.checkwrong(line)
    line = "select get_json_double('{}', $.k1'')"
    runner.checkwrong(line)


def test_query_get_json_int():
    """
    {
    "title": "test_query_string_function.test_query_get_json_int",
    "describe": "get_json_int(),hive对应的是get_json_object, 精度在11位小数",
    "tag": "function,p0,fuzz"
    }
    """
    """get_json_int(),hive对应的是get_json_object
        精度在11位小数
    """
    line1 = "SELECT get_json_int('{\"k1\":111, \"k2\":\"2\"}', '$.k1')"
    line2 = "select 111"
    runner.check2(line1, line2)
    ##精度验证,hive的精度很高很高,20位+.......
    line1 = "SELECT get_json_int('{\"k1\":-1234567890, \"k2\":\"2\"}', \"$.k1\"), \
        get_json_int('{\"k1\":111, \"k2\":+12345678}', \"$.k2\"),\
        get_json_int('{\"k1\":111, \"k2\":1234567890}', \"$.k2\"),\
        get_json_int('{\"k1\":111, \"k2\":22.1234567890}', \"$.k2\"),\
        get_json_int('{\"k1\":111111111111.33333333333333, \"k2\":123456789012}', \"$.k2\")"
    line2 = "select -1234567890, null, 1234567890, null, null"
    runner.check2(line1, line2)
    ##其他类型的数值
    line1 = " SELECT get_json_int('{\"k1\":1.3, \"k2\":\"2222\"}', \"$.k2\"), \
        get_json_int('{\"k1\":1.3, \"k2\":2888}', \"%.k2\"),\
        get_json_double('{\"k1\":1.3, \"k2\":2888}', \"$.k2\"),\
        get_json_int('{\"k1\":1.3, \"k2\":22888}', \"$.k3\")"
    line2 = "select null, null, 2888.0, null"
    runner.check2(line1, line2)
    ##json是列表;列表的嵌套不能识别
    line1 = "SELECT get_json_int('{\"k1\":\"v1\", \"my.key\":[1.1, 22, 3.3]}', '$.\"my.key\"[1]'),\
           get_json_int('{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}', '$.\"my.key\"[8]'),\
           get_json_int('{\"k1\":\"v1\", \"my.key\":[11, 2.2, 3.3]}', '$.\"my.key\"[0]'), \
           get_json_int('{\"k1\":\"v1\", \"my.key\":[11, 2.2, 3.3], \
             \"my.key\":[1.1, 2.2, 3.3]}',  '$.\"my.key\"[0]'), \
           get_json_int('{\"k1\":\"v1\", \"my.key\":[]}', '$.\"my.key\"[0]')"
    line2 = "select 22, null, 11, 11, null"
    runner.check2(line1, line2)

    line1 = "select get_json_int('{\"k1\":\"v1\", \"my.key\":[11, 22, 33, 44]}', '$.\"my.key\"[-1]'), \
         get_json_int('{\"k1\":\"v1\", \"my.key\":[11, 2.2], \"my.key\":[1.1, 2.2]}', '$.\"my.key\"[0]'), \
         get_json_int('{\"k1\":\"v1\", \"my.key\":[[1.1, 2.2], 333, 4.4]}', '$.\"my.key\"[1]'),\
         get_json_int('{\"k1\":\"v1\", \"my.key\":[3.3, [1.1, 2.2], 4.4]}', '$.\"my.key\"[1]')"
    line2 = "select null, 11, 333, null"
    runner.check2(line1, line2)

    line1 = "select get_json_int('{\"k1\":{\"k1\": [1,2,3]}}', '$.\"k1\".k1.[0]')"
    line2 = "select 1"
    runner.check2(line1, line2)
    line1 = "select get_json_int('{\"k1\":1},{\"k1\":2},{\"k2\":2}}', '$.k1')"
    line2 = "select null"
    runner.check2(line1, line2)
    line1 = "select get_json_int('{\"k1\":1},{\"k1\":[2,2]},{\"k2\":2}}', '$.k1')"
    line2 = "select null"
    runner.check2(line1, line2)
    ##checkwrong
    line = "SELECT get_json_int('{\"k1\":1111.1111, \"k2\":222}', '$.k1', '$.k2')"
    runner.checkwrong(line)
    line = "select get_json_int('{}', $.k1'')"
    runner.checkwrong(line)


def test_query_get_json_string():
    """
    {
    "title": "test_query_string_function.test_query_get_json_string",
    "describe": "get_json_tring(),hive对应的是get_json_object,输入int double返回为值",
    "tag": "function,p0,fuzz"
    }
    """
    """get_json_tring(),hive对应的是get_json_object,输入int double返回为值
    """
    line1 = "SELECT get_json_string('{\"k1\":\"111\", \"k2\":\"2\"}', '$.k1')"
    line2 = "select 111"
    runner.check2(line1, line2)
    ##字典、列表嵌套验证
    line1 = "SELECT get_json_string('{\"k1\":-333333, \"k2\":[\"2\", \"hello\"]}', '$.\"k2\"[1]'), \
        get_json_string('{\"k1\":111, \"k2\":[\"+++2\", \"hello\"]}', '$.\"k2\"[0]'),\
        get_json_string('{\"k1\":111, \"k2\":[\"+++2\", \"hello\"]}', '$.\"k2\"[:1]'),\
        get_json_string('{\"k1\":111, \"k2\":{\"k1\":[\"hello\", \"world\"]}}', '$.\"k2\".k1[0]'),\
        get_json_string('{\"k1\":111, \"k2\":{\"k1\":{\"k1\":[\"hello\", \"world\"]}}}', '$.\"k2\".k1.k1[0]'),\
        get_json_string('{\"k1\":{\"k1\":[\"hello\", \"world\"]}, \"k2\":\
          {\"k1\":[\"hello\", \"world\"]}}', '$.\"k1\".k1[0]')"
    line2 = "select 'hello', '+++2', null, 'hello', 'hello', 'hello'"
    runner.check2(line1, line2)
    # int&double类型
    line1 = "select  get_json_string('{\"k1\":111, \"k2\":22.222}', \"$.k2\"),\
        get_json_string('{\"k1\":1111111.3333, \"k2\":89.89}', \"$.k2\"),\
        get_json_string('{\"k1\":1111111.3333, \"k2\":\"2019-09-08 00:09:09\"}', \"$.k2\"),\
        get_json_string('{\"k1\":1111111.3333, \"k2\":\"hello word\"}', \"$.k2\"),\
        get_json_string('{\"k1\":1111111.3333, \"k2\":\"true\"}', \"$.k2\")"
    line2 = 'select "22.222", "89.89", "2019-09-08 00:09:09", "hello word", "true"'
    runner.check2(line1, line2)
    # 长字符串
    line1 = "SELECT get_json_string('{\"k1\":1111.11, \"k2\":\
        \"abcdefghjklmnopqrstuvwxyzabcdefghjklmnopqrstuvwxyzabcdefghjklmnopqrstuvwxyz\"}', '$.k2')"
    line2 = "select 'abcdefghjklmnopqrstuvwxyzabcdefghjklmnopqrstuvwxyzabcdefghjklmnopqrstuvwxyz'"
    runner.check2(line1, line2)
    ##checkwrong
    line = "SELECT get_json_string('{\"k1\":1111.1111, \"k2\":222}', '$.k1', '$.k2')"
    runner.checkwrong(line)
    line = "select get_json_string(k11:11) from %s" % join_name
    runner.checkwrong(line)
    ###函数
    line1 = '''select get_json_string('{\"k1\":1111.1, \"k2\":repeat(\"abcdefaaaa\", 3)}', '$.k2'),\
          repeat(\"abcdefaaaa\", 3), get_json_string('{\"k1\":1111.1, \"k2\":concat(\"abcdefaaaa\", 30)}', '$.k2'),
         concat(\"abcdefaaaa\", 30), get_json_string('{\"k1\":1111.1, \"k2\":split_part(\"abcd efaaaa\", \" \", 1)}',\
          '$.k2'), split_part(\"abcd efaaaa\", \" \", 1)'''
    line2 = "select null, 'abcdefaaaaabcdefaaaaabcdefaaaa', null, 'abcdefaaaa30', null, 'abcd'"
    runner.check2(line1, line2)
    # checkwrong
    line = "select get_json_string('{}', $.k1'')"
    runner.checkwrong(line)


def test_query_split_part():
    """
    {
    "title": "test_query_string_function.test_query_split_part",
    "describe": "plit_part(),mysql对应的是substring_index,但有区别，mysql是取前几个,测试点：各类型；嵌套；支持的分隔符；字符串长度；表字段+-字符,重复的分隔符；超出分隔符范围",
    "tag": "function,p0,fuzz"
    }
    """
    """split_part(),mysql对应的是substring_index,但有区别，mysql是取前几个
        测试点：各类型；嵌套；支持的分隔符；字符串长度；表字段+-字符
        重复的分隔符；超出分隔符范围
    """
    res1 = (('', 1, ''), (None, 2, '2'))
    res2 = ((None, -32767, '-32767'), (None, -32767, '-32767'))
    res3 = (('-2', -2147483647, '-2'), ('-2', -2147483647, '-2'))
    res4 = ((None, -9223372036854775807, '-9223372036854775807'), \
            (None, -9223372036854775807, '-9223372036854775807'))
    # res5 = ((None, ('-654.654'), '-654.654'), (None, ('-258.369'), '-258.369'))
    res6 = ((None, 'false', 'false'), (None, 'false', 'false'))
    res7 = ((None, '', ''), (None, ' ', ' '))
    res8 = (('-', -123456.54, '-'), ('-987.00', -987.001, '-987.00'))
    res9 = ((None, -365.0, '-365'), ('-0.00', -0.001, '-0.00'))
    # res10 = (('', '1901-12-31', ''), ('', '1988-03-21', ''))
    # res11 = (('', '1901-01-01 00:00:00', ''), ('', '1989-03-21 13:00:00', ''))
    # 和mysql处理的不太一样，如select split_part('1999','1',2); palo->999,mysql is 1999
    for index in range(11):
        if index in [4, 9, 10]:
            # decimal, date, datetime
            continue
        line = " select split_part(k%s, '1', 1), k%s, split_part(concat(k%s, '12'), '1', 1) \
        from %s order by k%s limit 2" % (index + 1, index + 1, index + 1, join_name, index + 1)
        res = runner.query_palo.do_sql(line)
        print(line, res, eval('res'"%s" % (index + 1)))
        assert eval('res'"%s" % (index + 1)) == res
    ##嵌套
    line1 = "select split_part(split_part('he,llo word', ' ', 1),',',1)"
    line2 = "SELECT substring_index(substring_index('he,llo word', ' ', 1),',',1)"
    runner.check2(line1, line2)
    line1 = "select split_part(split_part((split_part('he lj;jj,lo wo rd', ' ', 2)), ',', 1), ';', 1)"
    line2 = "select 'lj'"
    # mysql不一致，但doris更好
    # line2 =  "select substring_index(substring_index((substring_index('he lj;jj,lo wo rd', ' ', 2)), ',', 1), ';', 1)"
    runner.check2(line1, line2)
    ##分隔符:2个空格；::; ;; , ;; , tab
    line1 = "select split_part('he  lj;jj','  ', 2), split_part('helj::jj,lo wo rd', '::', 2),\
            split_part('helj;;jj,lowo;;rd', ';;', 2),split_part('helj ;;jj,lowo ;;rd', ';; ', 2),\
            split_part('helj    jj,lowo ;;rd', '    ', 2)"
    line2 = "select 'lj;jj', 'jj,lo wo rd', 'jj,lowo', null, 'jj,lowo ;;rd'"
    runner.check2(line1, line2)
    ##不匹配，doris返回NULL
    line1 = "select split_part('he, llo word', ', ,', 1), split_part('heljjj,lowo;;rd', ',,', 2)"
    line2 = "select null, null"
    runner.check2(line1, line2)
    ##超过分隔边界值
    line1 = "select split_part('he, llo word', ', ,', 9), split_part('heljjj,lowo;;rd', ',', -1)"
    line2 = "select null, 'lowo;;rd'"
    runner.check2(line1, line2)
    ##字符串长度
    line1 = "select split_part(repeat('abcdef aaaa', 30), ' ', 20), repeat('abcdef aaaa', 3)"
    line2 = "select 'aaaaabcdef', 'abcdef aaaaabcdef aaaaabcdef aaaa'"
    runner.check2(line1, line2)
    line1 = "select split_part('具有重要的d意义', '的', 2)"
    line2 = 'select "d意义"'
    runner.check2(line1, line2)
    ##函数
    line1 = "select split_part(concat('abcdef aaaa', 'ss ss'), ' ', 20), concat('abcdef aaaa', 'ss ss')"
    line2 = "select null, 'abcdef aaaass ss'"
    runner.check2(line1, line2)
    ##checkwrong
    line = "select split_part((split_part('he lj;jj,lo wo rd', ' ', 2) as n), ',', 1)"
    runner.checkwrong(line)
    line = "select split_part(concat('he, llo word','ss ss') as n, ',', 1)"
    runner.checkwrong(line)


def test_query_money_format():
    """
    {
    "title": "test_query_string_function.test_query_money_format",
    "describe": "money_format,测试点：各类型；嵌套；数字长度；函数",
    "tag": "function,p0,fuzz"
    }
    """
    """money_format,
        测试点：各类型；嵌套；数字长度；函数
    """
    for index in range(11):
        # double精度问题，跳过测试用例
        if index in [3]:
            continue
        # 聚合函数
        line = "select money_format(max(k%s)), money_format(min(k%s)) from %s" \
               % (index + 1, index + 1, join_name)
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        excepted = util.read_file('../data/query_string_money_format.txt', 2 * (index + 1))
        print("line %s, actual %s, excepted %s" % (line, res, excepted))
        excepted = tuple(eval(excepted))
        util.check_same(res, excepted)
        ##非聚合函数
        line = "select k%s, money_format(k%s), money_format(k%s / 10) from %s \
          order by k%s limit 1" % (index + 1, index + 1, index + 1, join_name, index + 1)
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        excepted = util.read_file('../data/query_string_money_format.txt', 2 * index + 1)
        print("line %s, actual %s, excepted %s" % (line, res, excepted))
        if index in [4, 9, 10]:
            continue
        excepted = tuple(eval(excepted))
        util.check_same(res, excepted)
    # 嵌套 不行,NULL
    line = "select money_format(money_format(123456789)), money_format((123456789 + 12)),\
             money_format((123456789 * 12)), money_format((123456789 / 12)),\
              money_format((123456789 %10)), money_format('123456789'), money_format(cast('23455' as double))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((None, u'123,456,801.00', u'1,481,481,468.00', u'10,288,065.75', u'9.00', \
                 u'123,456,789.00', u'23,455.00'),)
    util.check_same(res, excepted)
    ###数字长度 19位
    line = "select money_format(repeat(1234567890,20)), money_format(repeat(123456789,2))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((None, u'123,456,789,123,456,789.00'),)
    util.check_same(res, excepted)
    # checkwrong
    line = "select money_format('123456789a')"
    runner.checkwrong(line)
    line = "select money_format(66777.888.00)"
    runner.checkwrong(line)
    line = "select select money_format(cast('23455' as char))"
    runner.checkwrong(line)


def test_query_regexp_extract():
    """
    {
    "title": "test_query_string_function.test_query_regexp_extract",
    "describe": "regexp_replace：字符串替换函数：hive的一样，第三个参数：0,1,2 https://www.cnblogs.com/skyEva/p/5175377.html 测试点：各类型；嵌套；数字长度；函数",
    "tag": "function,p0,fuzz"
    }
    """
    """regexp_replace：字符串替换函数：hive的一样，第三个参数：0,1,2
        https://www.cnblogs.com/skyEva/p/5175377.html
        测试点：各类型；嵌套；数字长度；函数
    """
    for index in range(11):
        if index in [5, 6]:
            line = "select k%s, regexp_extract(k%s, '([[:alpha:]]*)([[:digit:]]*)', 1) from %s \
              order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        else:
            line = "select k%s, regexp_extract(k%s, '([[:alpha:]]*)([[:digit:]]*)', 2) from %s \
               order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        print(line, res)
        excepted = util.read_file('../data/query_string_regexp_extract.txt', index + 1)
        print("line %s, actual %s, excepted %s" % (line, res, excepted))
        if index not in [4, 9, 10]:
            # decimal,datetime
            excepted = tuple(eval(excepted))
            util.check_same(res, excepted)
    ##与hive 不一致,hive是空，doris是正确值
    line = "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1)"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'b',),)
    util.check_same(res, excepted)
    line = "SELECT regexp_extract('x=a3&x=18abc&x=2&y=3&x=4','x=([0-9]+)([a-z]+)',0),\
         regexp_extract('x=a3&x=18abc&x=2&y=3&x=4','^x=([a-z]+)([0-9]+)',0) "
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'x=18abc', u'x=a3'),)
    util.check_same(res, excepted)
    ###
    line = " select regexp_extract('hitdecisiondlist','(i)(.*?)(e)',0),\
        regexp_extract('hitdecisiondlist','(i)(.*?)(e)',1),\
        regexp_extract('hitdecisiondlist','(i)(.*?)(e)',+1),\
        regexp_extract('hitdecisiondlist','(i)(.*?)(e)',2),\
        regexp_extract('hitdecisiondlist','(i)(.*?)(e)',3)"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'itde', u'i', u'i', u'td', u'e'),)
    util.check_same(res, excepted)
    ##配合其他的规则
    line = "select regexp_extract(\"['a1s1sdff']\",'[[:alpha:]]',0),\
         regexp_extract(\"['assdff','Ass']\",'[[:upper:]]',0),\
         regexp_extract(\"['assdff','Ass']\",'[[:punct:]]',0)"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    ##超过边界,为空
    line = "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 3),\
           regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', -2),\
            regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 9999)"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'', u'', u''),)
    util.check_same(res, excepted)
    ##checkwrong
    line = "SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', a)"
    runner.checkwrong(line)


def test_query_regexp_replace():
    """
    {
    "title": "test_query_string_function.test_query_regexp_replace",
    "describe": "regexp_replace：字符串替换函数：mysql：replace('wwwwwww.hh','w',"W"), 测试点：各类型；嵌套；数字长度；函数",
    "tag": "function,p0,fuzz"
    }
    """
    """"regexp_replace：字符串替换函数：mysql：replace('wwwwwww.hh','w',"W")
        测试点：各类型；嵌套；数字长度；函数
    """
    for index in range(11):
        line = "select k%s, regexp_replace(k%s,  '1', '--') from %s \
          order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        excepted = util.read_file('../data/query_string_regexp_replace.txt', index + 1)
        print("line %s, actual %s, excepted %s" % (line, res, excepted))
        if index not in [4, 9, 10]:
            # decimal,datetime
            excepted = tuple(eval(excepted))
            util.check_same(res, excepted)
    ##doris独有的
    line = "SELECT regexp_replace('bb,b', '^b', '-'), regexp_replace('bb,b', 'b$', '-'),\
            regexp_replace('bb,b', '[a|b]', '-'),regexp_replace('bb,b', 'b{2}', '-')"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    print(((u'-b,b', u'bb,-', u'--,-', u'-,b'),) == res)
    line1 = "SELECT regexp_replace('a b c', ' ', '-')"
    line2 = "SELECT replace('a b c', ' ', '-')"
    runner.check2(line1, line2)
    ##嵌套
    line1 = "SELECT regexp_replace(regexp_replace(split_part('aaaa bb,b c',' ', 2), ',', '-'),'-','=')"
    line2 = "SELECT replace(replace('bb,b', ',', '-'),'-','=')"
    runner.check2(line1, line2)
    ##函数
    line1 = "SELECT regexp_replace(concat('a b c','222'), ' ', '-'), \
             regexp_replace(concat('a b c','222'), ',', '-'),\
             regexp_replace(split_part('aaaa bb,b c',' ', 2), ',', '-'),split_part('aaaa bb,b c',' ', 2)"
    line2 = "SELECT replace(concat('a b c','222'), ' ', '-'),\
              replace(concat('a b c','222'), ',', '-'),\
            replace('bb,b', ',', '-'), 'bb,b'"
    runner.check2(line1, line2)
    ##匹配不到
    line1 = "SELECT regexp_replace('bb,b', '==', '-')"
    line2 = "SELECT replace('bb,b', '==' ,'-')"
    runner.check2(line1, line2)
    line = "SELECT regexp_replace('bb,b', ' ', '-','==')"
    runner.checkwrong(line)


def test_char_length():
    """
    {
    "title": "test_query_string_function.test_char_length",
    "describe": "",
    "tag": "function,p0,fuzz"
    }
    """
    line = "select char_length(k6), char_length(k7) from %s order by 1, 2" % table_name
    runner.check(line)
    line = "select char_length('hello'), char_length('测试'), char_length('this is a 测试')"
    runner.check(line)
    line = "select character_length(k6), character_length(k7) from %s order by 1, 2" % table_name
    runner.check(line)
    line = "select character_length('hello'), character_length('测试'), character_length('this is a 测试')"
    runner.check(line)


def test_ends_with():
    """
    {
    "title": "test_query_string_function.test_ends_with",
    "describe": "",
    "tag": "function,p0,fuzz"
    }
    """
    line1 = 'select k6, k7, ends_with(lower(k6), "e"), ends_with(lower(k7), "h") from %s ' \
            'order by k1, k2, k3, k4' % table_name
    line2 = 'select k6, k7, lower(k6) like "%%e", lower(k7) like "%%h" from %s order by k1, k2, k3, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select ends_with("我有一头小毛驴lala", "lala"), ends_with("我有一头小毛驴", "小毛驴"), ' \
            'ends_with("我有一头小毛驴", "lala")'
    line2 = 'select "我有一头小毛驴lala" like "%%lala", "我有一头小毛驴" like "%%小毛驴",' \
            '"我有一头小毛驴" like "%%lala"'
    runner.check2(line1, line2)


def test_null_or_empty():
    """
    {
    "title": "test_query_string_function.test_null_or_empty",
    "describe": "",
    "tag": "function,p0,fuzz"
    }
    """
    line1 = "select null_or_empty(null), null_or_empty('')"
    line2 = "select 1, 1"
    runner.check2(line1, line2)
    line1 = "select null_or_empty('hello'), null_or_empty('测试')"
    line2 = "select 0, 0"
    runner.check2(line1, line2)


def test_starts_with():
    """
    {
    "title": "test_query_string_function.test_starts_with",
    "describe": "",
    "tag": "function,p0,fuzz"
    }
    """
    line1 = 'select k6, k7, starts_with(lower(k6), "f"), starts_with(lower(k7), "g") from %s ' \
            'order by k1, k2, k3, k4' % table_name
    line2 = 'select k6, k7, lower(k6) like "f%%", lower(k7) like "g%%" from %s order by k1, k2, k3, k4' % table_name
    runner.check2(line1, line2)
    line1 = 'select starts_with("lala我有一头小毛驴lala", "lala我"), starts_with("我有一头小毛驴", "我有一头小毛驴"), ' \
            'starts_with("我有一头小毛驴", "lala")'
    line2 = 'select "lala我有一头小毛驴lala" like "lala我%%", "我有一头小毛驴" like "我有一头小毛驴%%", ' \
            '"我有一头小毛驴" like "lala%%"'
    print(line1)
    runner.check2(line1, line2)


def test_string_space():
    """
    {
    "title": "test_query_string_function.test_string_space",
    "describe": "space()函数测试",
    "tag": "function,p0,fuzz"
    }
    """
    line = "select space(5), space(0), space(-1)"
    runner.check(line)
    line = "select space('a')"
    runner.checkwrong(line)


def test_string_append_trailing_char_if_absent():
    """
    {
    "title": "test_query_string_function.test_string_append_trailing_char_if_absent",
    "describe": "向字符串末尾加入单个字符",
    "tag": "function,p0"
    }
    """
    line1 = "select append_trailing_char_if_absent('a', 'c'), append_trailing_char_if_absent('abcd', 'c')"
    line2 = "select 'ac', 'abcdc'"
    runner.check2(line1, line2)
    line1 = "select append_trailing_char_if_absent('c', 'c'), append_trailing_char_if_absent('abc', 'c')"
    line2 = "select 'c', 'abc'"
    runner.check2(line1, line2)
    line1 = "select append_trailing_char_if_absent('a ', 'c'), append_trailing_char_if_absent('abcd ', ' ')"
    line2 = "select 'a c', 'abcd '"
    runner.check2(line1, line2)
    line1 = "select append_trailing_char_if_absent('abcd', 'cd'), append_trailing_char_if_absent('abcd', 'ee')"
    line2 = "select NULL, NULL"
    runner.check2(line1, line2)


def test_string_bit_length():
    """
    {
    "title": "test_query_string_function.test_string_bit_length",
    "describe": "返回字符串的位长度",
    "tag": "function,p0"
    }
    """
    line = "select bit_length(k6) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select bit_length(k7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select bit_length(k1) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)


def test_string_md5():
    """
    {
    "title": "test_query_string_function.test_string_md5",
    "describe": "字符串转换为md5格式",
    "tag": "function,p0"
    }
    """
    line = "select md5(k6) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select md5(k7) from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line1 = "select md5sum(k6) from %s order by k1,k2,k3,k4" % table_name
    line2 = "select md5(k6) from %s order by k1,k2,k3,k4" % table_name
    runner.check2(line1, line2)
    line1 = "select md5sum(k7) from %s order by k1,k2,k3,k4" % table_name
    line2 = "select md5(k7) from %s order by k1,k2,k3,k4" % table_name
    runner.check2(line1, line2)


def test_string_murmur_hash3_32():
    """
    {
    "title": "test_query_string_function.test_string_murmur_hash3_32",
    "describe": "返回输入字符串的32位murmur3 hash值",
    "tag": "function,p0"
    }
    """
    line1 = "select murmur_hash3_32(null)"
    line2 = "select NULL"
    runner.check2(line1, line2)
    line1 = "select murmur_hash3_32('hello')"
    line2 = "select 1321743225"
    runner.check2(line1, line2)
    line1 = "select murmur_hash3_32('hello', 'world')"
    line2 = "select 984713481"
    runner.check2(line1, line2)


def test_string_digital_masking():
    """
    {
    "title": "test_query_string_function.test_string_digital_masking",
    "describe": "返回遮盖脱敏后的结果，同concat(left(id,3),'****',right(id,4))",
    "tag": "function,p0"
    }
    """
    line1 = "select digital_masking(13012345678)"
    line2 = "select concat(left(13012345678,3),'****',right(13012345678,4))"
    runner.check2(line1, line2)
    line1 = "select digital_masking(13000000000)"
    line2 = "select concat(left(13000000000,3),'****',right(13000000000,4))"
    runner.check2(line1, line2)
    line1 = "select digital_masking(130123456789)"
    line2 = "select concat(left(130123456789,3),'****',right(130123456789,4))"
    runner.check2(line1, line2)
    line1 = "select digital_masking(1301234567)"
    line2 = "select concat(left(1301234567,3),'****',right(1301234567,4))"
    runner.check2(line1, line2)
    line1 = "select digital_masking(130)"
    line2 = "select concat(left(130,3),'****',right(130,4))"
    runner.check2(line1, line2)


def test_encrypt_decrypt():
    """
    {
    "title": "test_query_string_function.test_encrypt_decrypt",
    "describe": "自定义密钥，加密解密操作, github issue #5954",
    "tag": "function,p0"
    }
    """
    sql = "drop encryptkey test_my_key"
    LOG.info(L('palo sql', palo_sql=sql))
    try:
        runner.query_palo.do_sql(sql)
    except:
        print("KEY test_my_key not exist")
    sql = "create encryptkey test_my_key as 'ABCD123456789'"
    LOG.info(L('palo sql', palo_sql=sql))
    runner.query_palo.do_sql(sql)
    line1 = "select hex(aes_encrypt('Doris is Great', key test_my_key))"
    line2 = "select 'D26DB38579D6A343350EDDC6F2AD47C6'"
    runner.check2(line1, line2)
    line1 = "select aes_decrypt(unhex(hex(aes_encrypt(k6, key test_my_key))), key test_my_key) from %s \
            order by k1,k2,k3,k4" % table_name
    line2 = "select k6 from %s order by k1,k2,k3,k4" % table_name
    runner.check2(line1, line2)
    sql = "drop encryptkey test_my_key"
    runner.query_palo.do_sql(sql)


def test_query_replace():
    """
    {
    "title": "test_query_string_function.test_query_replace",
    "describe": "test replace function, github issue #6604",
    "tag": "function,p1"
    }
    """
    line = "select replace(k6, 'a', 'b') from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select replace(k7, 'bc','bb') from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select replace(k7, 'xyz', '') from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select replace(k7, 'w', 'W') from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    line = "select replace(k7, 'w', 'w') from %s order by k1,k2,k3,k4" % table_name
    runner.check(line)
    # github issue #6604
    line = "select replace('http://www.baidu.com:9090','http', 'ftp')"
    runner.check(line)


def teardown_module():
    """
    end 
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()


if __name__ == "__main__":
    print("test")
    setup_module()
    test_query_money_format()
    test_query_regexp_extract()
    test_query_regexp_replace()
