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

table_name = "test"
join_name = "baseall"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()
    
    
def test_query_math_round():
    """
    {
    "title": "test_query_numeric_function.test_query_math_round",
    "describe": "test for math function round",
    "tag": "function,p0"
    }
    """
    """
    test for math function round
    """
    line = "select round(k1, -1), round(k2, -1), round(k3, -1), k3, round(k5, -1), round(k8, -1), round(k9, -1) " \
           "from %s where abs(k1%%10) != 5 and abs(k2%%10) != 5 and abs(k3%%10) != 5 and " \
           "abs(abs(k5%%10) - 5) > 0.01 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select round(k1), round(k2), round(k3), round(k5), round(k8), round(k9) \
    		     from %s where abs(k1*10%%10) != 5 and abs(k2*10%%10) != 5 and abs(k3*10%%10) != 5 and abs(abs(k5*10%%10) - 5) > 0.01 \
		     order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select round(k1, 0), round(k2, 0), round(k3, 0), round(k5, 0), round(k8, 0), round(k9, 0) \
    		     from %s where abs(k1*10%%10) != 5 and abs(k2*10%%10) != 5 and abs(k3*10%%10) != 5 and abs(abs(k5*10%%10) - 5) > 0.01 \
		     order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select round(k1, 1), round(k2, 1), round(k3, 1), round(k5, 1), round(k8, 1), round(k9, 1) \
    		     from %s where abs(k1*100%%10) != 5 and abs(k2*100%%10) != 5 and abs(k3*100%%10) != 5 and abs(abs(k5*100%%10) - 5) > 0.01 \
		     order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    # issue 7420
    line = "select round(0.5,0), round(222450.00,-2)"
    runner.check(line)


def test_query_math_truncate():
    """
    {
    "title": "test_query_numeric_function.test_query_math_truncate",
    "describe": "test for math function truncate",
    "tag": "function,p0"
    }
    """
    """
    test for math function truncate
    """
    line = "select truncate(k1, -1), truncate(k2, -1), truncate(k3, -1), k3, truncate(k5, -1), truncate(k8, -1), truncate(k9, -1) \
                 from %s where abs(k1%%10) != 5 and abs(k2%%10) != 5 and abs(k3%%10) != 5 and abs(abs(k5%%10) - 5) > 0.01 \
             order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select truncate(k1, 0), truncate(k2, 0), truncate(k3, 0), truncate(k5, 0), truncate(k8, 0), truncate(k9, 0) \
                 from %s where abs(k1*10%%10) != 5 and abs(k2*10%%10) != 5 and abs(k3*10%%10) != 5 and abs(abs(k5*10%%10) - 5) > 0.01 \
             order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select truncate(k1, 1), truncate(k2, 1), truncate(k3, 1), truncate(k5, 1), truncate(k8, 1), truncate(k9, 1) \
                 from %s where abs(k1*100%%10) != 5 and abs(k2*100%%10) != 5 and abs(k3*100%%10) != 5 and abs(abs(k5*100%%10) - 5) > 0.01 \
             order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_abs():
    """
    {
    "title": "test_query_numeric_function.test_query_math_abs",
    "describe": "test for math function abs",
    "tag": "function,p0"
    }
    """
    """
    test for math function abs
    """
    line = "select k1, abs(k2), abs(k5), abs(k8), abs(k9) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)

    
def test_query_math_conv():
    """
    {
    "title": "test_query_numeric_function.test_query_math_conv",
    "describe": "test for math function conv",
    "tag": "function,p0"
    }
    """
    """
    test for math function conv
    """
    for i in range(1, 5):
        line = "select k1, conv(k%s, 10, 2) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select k1, conv(k%s, 10, 8) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
        line = "select k1, conv(k%s, 10, 16) from %s order by k1, k2, k3, k4" % (i, table_name)
        runner.check(line)
    for i in (7, 6):
        line = "select k1, conv(k%s, 36, 2) from %s where lower(k%s) \
			regexp '^[a-z]+[0-9]*$' order by k1, k2, k3, k4" % (i, table_name, i)
        runner.check(line)
        line = "select k1, conv(k%s, 36, 8) from %s where lower(k%s) \
			regexp '^[a-z]+[0-9]*$' order by k1, k2, k3, k4" % (i, table_name, i)
        runner.check(line)
        line = "select k1, conv(k%s, 36, 16) from %s where lower(k%s) \
			regexp '^[a-z]+[0-9]*$' order by k1, k2, k3, k4" % (i, table_name, i)
        runner.check(line)


def test_query_math_acos_asin_atan():
    """
    {
    "title": "test_query_numeric_function.test_query_math_acos_asin_atan",
    "describe": "test for math function acos, asin, atan",
    "tag": "function,p0"
    }
    """
    """
    test for math function acos, asin, atan
    """
    line = "select k1, acos(k8) from %s where abs(k8)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, asin(k8) from %s where abs(k8)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, acos(k9) from %s where abs(k9)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, asin(k9) from %s where abs(k9)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, acos(k5) from %s where abs(k5)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, asin(k5) from %s where abs(k5)<1 order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, atan(k2), atan(k3), atan(k4), atan(k5), atan(k8), \
		    atan(k9) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_bin():
    """
    {
    "title": "test_query_numeric_function.test_query_math_bin",
    "describe": "test for math function bin",
    "tag": "function,p0"
    }
    """
    """
    test for math function bin
    """
    line = "select k1, k8, bin(k1), bin(k2), bin(k3), bin(k4), bin(k5), bin(k8), \
    		    bin(k9) from %s order by k1, k2, k3, k4" % (table_name)
    line = "select k1, k8, bin(k1), bin(k2), bin(k3), bin(k4) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_ceil():
    """
    {
    "title": "test_query_numeric_function.test_query_math_ceil",
    "describe": "test for math function ceil, ceiling, dceil",
    "tag": "function,p0"
    }
    """
    """
    test for math function ceil, ceiling
    """
    line = "select k1, ceil(k2), ceil(k3), ceil(k5), ceil(k8), ceil(k9)\
		     from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, ceiling(k2), ceiling(k3), ceiling(k5), ceiling(k8),\
		    ceiling(k9) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select k1, dceil(k2), dceil(k3), dceil(k5), dceil(k8), dceil(k9)\
             from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2_palo(line, line2)


def test_query_math_floor():
    """
    {
    "title": "test_query_numeric_function.test_query_math_floor",
    "describe": "test for math function floor, dfloor",
    "tag": "function,p0"
    }
    """
    """
    test for math function floor
    """
    line = "select k1, floor(k2), floor(k3), floor(k5), floor(k8), floor(k9) \
		     from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select k1, dfloor(k2), dfloor(k3), dfloor(k5), dfloor(k8), dfloor(k9) \
             from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)


def test_query_math_cos_sin_tan():
    """
    {
    "title": "test_query_numeric_function.test_query_math_cos_sin_tan",
    "describe": "test for math function cos, sin, tan",
    "tag": "function,p0"
    }
    """
    """
    test for math function cos, sin, tan
    """
    line = "select k1, cos(k2), cos(k3), cos(k4), cos(k5), cos(k8), cos(k9)\
		     from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, sin(k2), sin(k3), sin(k4), sin(k5), sin(k8), sin(k9) \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, tan(k2), tan(k3), tan(k4), tan(k5), tan(k8), tan(k9) \
		    from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_degrees():
    """
    {
    "title": "test_query_numeric_function.test_query_math_degrees",
    "describe": "test for math function degrees",
    "tag": "function,p0"
    }
    """
    """
    test for math function degrees
    """
    line = "select k1, degrees(k2), degrees(k3), degrees(k4), degrees(k5), degrees(k8), \
		    degrees(k9) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_exp():
    """
    {
    "title": "test_query_numeric_function.test_query_math_exp",
    "describe": "test for math function exp, dexp",
    "tag": "function,p0"
    }
    """
    """
    test for math function exp
    """
    line = "select k1, exp(k1) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select k1, dexp(k1) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)


def test_query_hex_math_unhex():
    """
    {
    "title": "test_query_numeric_function.test_query_hex_math_unhex",
    "describe": "test for math function hex, unhex",
    "tag": "function,p0"
    }
    """
    """
    test for math function hex, unhex
    """
    line = "select hex(k1), hex(k2), hex(k3), hex(k4), hex(k6), hex(k7) from %s \
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select unhex(hex(k6)), unhex(hex(k7)) from %s order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)


def test_query_math_ln_log():
    """
    {
    "title": "test_query_numeric_function.test_query_math_ln_log",
    "describe": "test for math function ln, log2, log10, log, dlog1, dlog10",
    "tag": "function,p0"
    }
    """
    """
    test for math function ln, log2, log10, log, dlog1, dlog10
    """
    line = "select ln(abs(k1)), ln(abs(k2)), ln(abs(k3)), ln(abs(k4)), ln(abs(k5)), ln(abs(k8)), \
		    ln(abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
		    and k8<>0 and k9<>0\
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select dlog1(abs(k1)), dlog1(abs(k2)), dlog1(abs(k3)), dlog1(abs(k4)), dlog1(abs(k5)), dlog1(abs(k8)), \
            dlog1(abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
            and k8<>0 and k9<>0\
            order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)
    line = "select log2(abs(k1)), log2(abs(k2)), log2(abs(k3)), log2(abs(k4)), log2(abs(k5)), \
		    log2(abs(k8)), log2(abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
		    and k8<>0 and k9<>0\
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select log10(abs(k1)), log10(abs(k2)), log10(abs(k3)), log10(abs(k4)), log10(abs(k5)),\
		    log10(abs(k8)), log10(abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
		    and k8<>0 and k9<>0\
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select dlog10(abs(k1)), dlog10(abs(k2)), dlog10(abs(k3)), dlog10(abs(k4)), dlog10(abs(k5)),\
            dlog10(abs(k8)), dlog10(abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
            and k8<>0 and k9<>0\
            order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)
    line = "select log(2, abs(k1)), log(2, abs(k2)), log(2, abs(k3)), log(2, abs(k4)), log(2, abs(k5)), \
		    log(2, abs(k8)), log(2, abs(k9)) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
		    and k8<>0 and k9<>0\
		    order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select log(abs(k1), abs(k1)), log(abs(k1), abs(k2)), log(abs(k1), abs(k3)), \
		    log(abs(k1), abs(k4)), log(abs(k1), abs(k5)), \
		    log(abs(k1), abs(k8)), log(abs(k1), abs(k9)) from %s \
		    where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 and k8<>0 and k9<>0\
		    order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select log(abs(k1), k1), log(abs(k1), abs(k2)), log(abs(k1), abs(k3)), \
		    log(abs(k1), abs(k4)), log(abs(k1), abs(k5)), \
		    log(abs(k1), abs(k8)), log(abs(k1), abs(k9)) from %s \
		    where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 \
		    and k8<>0 and k9<>0 order by k1, k2, k3, k4" \
		    % (table_name)
    runner.check(line)
    line = "select log(1,2), log(1,1), log(0,2), log(2,0), log(0,0)"
    runner.check(line)


def test_query_math_negative():
    """
    {
    "title": "test_query_numeric_function.test_query_math_negative",
    "describe": "test for math function negative",
    "tag": "function,p0"
    }
    """
    """
    test for math function negative
    """
    line1 = "select k1, negative(k2), negative(k3), negative(k4), negative(k8), negative(k9),\
		     negative(k5) from %s order by k1, k2, k3, k4" % (table_name)
    line2 = "select k1, -k2, -k3, -k4, -k8, -k9,  -k5 from %s order by k1, k2, k3, k4"\
		    % (table_name)
    print(line1)
    print(line2)
    runner.check2(line1, line2)


def test_query_math_pow():
    """
    {
    "title": "test_query_numeric_function.test_query_math_pow",
    "describe": "test for math function pow &  power & dpow & fpow",
    "tag": "function,p0"
    }
    """
    """
    test for math function pow &  power & dpow & fpow
    """
    line = "select k1, pow(2, k1), pow(-2, k1),\
            pow(k1, 2), pow(k2, 2), pow(k3, 2), pow(k4, 2), pow(k5, 2),\
            pow(k8, 2), pow(k9, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, power(2, k1), power(-2, k1),\
                power(k1, 2), power(k2, 2), power(k3, 2), power(k4, 2), power(k5, 2),\
                power(k8, 2), power(k9, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select k1, dpow(2, k1), dpow(-2, k1),\
            dpow(k1, 2), dpow(k2, 2), dpow(k3, 2), dpow(k4, 2), dpow(k5, 2),\
            dpow(k8, 2), dpow(k9, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)
    line3 = "select k1, fpow(2, k1), fpow(-2, k1),\
            fpow(k1, 2), fpow(k2, 2), fpow(k3, 2), fpow(k4, 2), fpow(k5, 2),\
            fpow(k8, 2), fpow(k9, 2) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line3, line)
    line = "select k1, pow(k1, -2), pow(k2, -2), pow(k3, -2), pow(k4, -2), pow(k5, -2),\
            pow(k8, -2), pow(k9, -2) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 \
            and k5<>0 and k8<>0 and k9<>0 \
            order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line = "select k1, power(k1, -2), power(k2, -2), power(k3, -2), power(k4, -2), power(k5, -2),\
            power(k8, -2), power(k9, -2) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 \
            and k5<>0 and k8<>0 and k9<>0 \
            order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select k1, dpow(k1, -2), dpow(k2, -2), dpow(k3, -2), dpow(k4, -2), dpow(k5, -2),\
            dpow(k8, -2), dpow(k9, -2) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 \
            and k5<>0 and k8<>0 and k9<>0 \
            order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)
    line3 = "select k1, fpow(k1, -2), fpow(k2, -2), fpow(k3, -2), fpow(k4, -2), fpow(k5, -2),\
            fpow(k8, -2), fpow(k9, -2) from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 \
            and k5<>0 and k8<>0 and k9<>0 \
            order by k1, k2, k3, k4" % (table_name)
    runner.check2(line3, line)


def test_query_math_radians():
    """
    {
    "title": "test_query_numeric_function.test_query_math_radians",
    "describe": "test for math function pow",
    "tag": "function,p0"
    }
    """
    """
    test for math function pow
    """
    line = "select radians(k1), radians(k2), radians(k3), radians(k4), radians(k5), radians(k8),\
		    radians(k9) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    
    
def test_query_math_sign():
    """
    {
    "title": "test_query_numeric_function.test_query_math_sign",
    "describe": "test for math function sign",
    "tag": "function,p0"
    }
    """
    """
    test for math function sign
    """
    line = "select sign(k1), sign(k2), sign(k3), sign(k4), sign(k5), sign(k8), sign(k9) "\
           "from %s where k1<>0 and k2<>0 and k3<>0 and k4<>0 and k5<>0 and k8<>0 and k9<>0 "\
           "order by k1, k2, k3, k4" % (table_name)
    runner.check(line)


def test_query_math_sqrt():
    """
    {
    "title": "test_query_numeric_function.test_query_math_sqrt",
    "describe": "test for math function sqrt, dsqrt",
    "tag": "function,p0"
    }
    """
    """
    test for math function sqrt, dsqrt
    """
    line = "select sqrt(abs(k1)), sqrt(abs(k2)), sqrt(abs(k3)), sqrt(abs(k4)), sqrt(abs(k5)), sqrt(abs(k8)), \
		    sqrt(abs(k9)) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select dsqrt(abs(k1)), dsqrt(abs(k2)), dsqrt(abs(k3)), dsqrt(abs(k4)), dsqrt(abs(k5)), dsqrt(abs(k8)), \
            dsqrt(abs(k9)) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)


def test_query_sum():
    """
    {
    "title": "test_query_numeric_function.test_query_sum",
    "describe": "query contains sum,sum one and group by another",
    "tag": "function,p0"
    }
    """
    """
    query contains sum
    sum one and group by another
    """
    line = "select sum(case lower(k6) when \"true\" then 2 else 0 end) from %s \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    line = "select sum(case lower(k6) when \"true\" then 2 when \"false\" then 1 else 0 end) from %s \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    for i in (1, 2, 3, 5, 8, 9):
        for j in (1, 2, 3, 5, 8, 9):
            if i != j:
                print(i, " ", j)
                line = "select sum(k%s) from %s where k%s<>0 group by k%s order by k%s"\
				% (i, table_name, j, j, j) 
                runner.check(line)
                line = "select sum(distinct k%s) from %s where k%s<>0 group by k%s order by k%s"\
				% (i, table_name, j, j, j) 
                runner.check(line)


def test_query_count_1():
    """
    {
    "title": "test_query_numeric_function.test_query_count_1",
    "describe": "test for function count",
    "tag": "function,p0"
    }
    """
    """
    test for function count
    """
    line = "select count(*) from %s where (case lower(k6) when \"true\" then \"hello\" when \"false\" then \"world\" end\
		    =\"hello\") group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    line1 = "select k1, count(*) from %s where case k1 when 125 then cast(1 as int) else cast(0 as int) end =1\
		    group by 1 order by 1" % (table_name)
    line2 = "select k1, count(*) from %s where case k1 when 125 then cast(1 as signed) else cast(0 as signed) end =1\
		    group by 1 order by 1" % (table_name)
    runner.check2(line1, line2)
    line = "select count(*) from %s where 10000 in (k1, k2, k3, k4)" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where 10000 not in (k1, k2, k3, k4)" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where case lower(k6) when \"true\" then (true) else (false) end \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where case lower(k6) when \"true\" then (false) else (true) end \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where case lower(k6) when \"true\" then (NULL) else (false) end \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
    line = "select k1, count(*) from %s where case k1 when 6 then (true) else (NULL) end \
		    group by 1 order by 1" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where case lower(k6) when NULL then (true) else (false) end \
		    group by lower(k6) order by 1" % (table_name)
    runner.check(line)
#   line = "select k1, count(*) from %s where case k1 when 1 then (true) else (NULL) end is null \
#   	    group by 1 order by 1" % (table_name)
#   check(line)
    line = "select k1, count(*) from %s where case k1 when 1 then (true) end is null \
		    group by 1 order by 1" % (table_name)
    runner.check(line)
    line = "select k1, count(*) from %s where case when (k1 = 1) then (true) when (k1 = 2) then (true) \
		    else (false) end group by 1 order by 1" % (table_name)
    runner.check(line, True)
    line = "select count(*) from %s where case lower(k7) when \"jiw3n4\" then (true) \
		    when \"yanvjldjlll\" then (false) when \"yunlj8@nk\" then (true) end \
		    group by lower(k7) order by 1" % (table_name)
    runner.check(line)
    line = "select count(*) from (select * from %s order by k1, k2, k3, k4 limit 10) a" \
		    % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k1 between 1 and 2" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k1 not between 1 and 2" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k2 between 1 and 1000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k2 not between 1 and 1000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k3 between 1 and 10000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k3 not between 1 and 10000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k4 between 1 and 100000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k4 not between 1 and 100000" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k5 between 1.0 and 100.0" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k5 not between 1 and 100" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k5 between 1.0 and 100.0" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k5 not between 1 and 100" % (table_name)
    runner.check(line)
    line = "select count(5) from (select * from %s) a" % table_name
    runner.check(line)


def test_query_count_2():
    """
    {
    "title": "test_query_numeric_function.test_query_count_2",
    "describe": "test for function count",
    "tag": "function,p0"
    }
    """
    """
    test for function count
    """
    line = "select count(*) from %s where k11 between cast('1989-03-21 00:00:00' as datetime) and \
		    cast('2013-03-21 00:00:00' as datetime)" % (table_name)
    runner.check(line)
    line = "select count(*) from %s where k11 not between cast('1989-03-21 00:00:00' as datetime) and \
		    cast('2013-03-21 00:00:00' as datetime)" % (table_name)
    runner.check(line)
    for i in range(1, 12):
        for j in range(1, 12):
            if i != j and i == 6 and j == 8:
                if j != 7 and j != 6 and i != 7 and i != 6:
                    line = "select count(k%s) from %s group by k%s order by k%s"\
			        % (i, table_name, j, j) 
                    line = "select count(distinct k%s) from %s where k%s<>0 \
				group by k%s order by k%s"\
				% (i, table_name, j, j, j) 
                    runner.check(line)
                else:
                    line = "select count(k%s) from %s \
		            where k7<>\"\" and k7<>\" \" and k6<>\"\" and k6<>\" \" \
		            group by k%s order by k%s" % (i, table_name, j, j) 
                    if (j == 7 or j == 6):
                        line = "select count(distinct k%s) from %s \
		                where k7<>\"\" and k7<>\" \" and k6<>\"\" and k6<>\" \" \
				group by k%s order by k%s"\
				% (i, table_name, j, j) 
                    else:
                        line = "select count(distinct k%s), k8 from %s \
		                where k%s<>0 and k7<>\"\" and k7<>\" \" and k6<>\"\" and k6<>\" \" \
				group by k%s order by k%s"\
				% (i, table_name, j, j, j) 

                    runner.check(line)


def test_query_stddev():
    """
    {
    "title": "test_query_numeric_function.test_query_stddev",
    "describe": "stddev",
    "tag": "function,p0"
    }
    """
    """stddev"""
    columns = ['k1', 'k2', 'k3', 'k4', 'k8', 'k9']
    for c in columns:
        line1 = 'select stddev(%s), variance(%s) from %s group by k1 order by k1' \
                % (c, c, table_name)
        line2 = 'select stddev(%s), variance(%s) from %s group by k1 order by k1' \
                % (c, c, table_name)
        runner.check2(line1, line2)
        line1 = 'select stddev_pop(%s), variance_pop(%s) from %s group by k1 order by k1' \
                % (c, c, table_name)
        line2 = 'select stddev_pop(%s), var_pop(%s) from %s group by k1 order by k1' \
                % (c, c, table_name)
        runner.check2(line1, line2)
    line1 = "select stddev(k1), stddev_pop(k2), stddev_samp(k3), variance(k1), variance_pop(k2), \
             variance_samp(k3) from %s where k11 between cast('1989-03-21 00:00:00' as datetime) \
             and cast('2013-03-21 00:00:00' as datetime)" % table_name
    line2 = "select stddev(k1), stddev_pop(k2), stddev_samp(k3), variance(k1), var_pop(k2), \
             var_samp(k3) from %s where k11 between cast('1989-03-21 00:00:00' as datetime) and \
             cast('2013-03-21 00:00:00' as datetime)" % (table_name)
    runner.check2(line1, line2)
    line1 = 'select stddev(k1) s1, variance(k1) from test where k2 > 0 and k3 < 0 group by k11 \
             order by negative(isnull(s1)), 1, 2'
    line2 = 'select stddev(k1) s1, variance(k1) from test where k2 > 0 and k3 < 0 group by k11 \
             order by 1, 2'
    runner.check2(line1, line2)
    line1 = 'select stddev(k1), variance(k1) from baseall where k1 > 20'
    line2 = 'select stddev(k1), variance(k1) from baseall where k1 > 20'
    runner.check2(line1, line2)
    line1 = 'select stddev(distinct k2), variance(distinct k2) from test group by k1 order by k1'
    line2 = 'select stddev(v), variance(v) from (select distinct k1, k2 v from test) a \
             group by k1 order by k1'
    runner.check2(line1, line2)
    

def test_query_positive():
    """
    {
    "title": "test_query_numeric_function.test_query_positive",
    "describe": "positive",
    "tag": "function,p0"
    }
    """
    """positive"""
    line1 = 'select positive(0), positive(-1), positive(-101), positive(1), positive(101)'
    line2 = 'select 0, -1, -101, 1, 101'
    runner.check2(line1, line2)


def test_query_greatest():
    """
    {
    "title": "test_query_numeric_function.test_query_greatest",
    "describe": "greatest & latest",
    "tag": "function,p0"
    }
    """
    """greatest & latest"""
    line = 'select greatest(k1, k2) from baseall order by k1'
    runner.check(line)
    line = 'select greatest(1, 2, 3, 4, 5, 6)'
    runner.check(line)
    line = 'select greatest(1.1, 1.2, 1.3, 1.4, 1.5, k9) from baseall order by k1'
    runner.check(line)
    lind = 'select greatest(1, 2, 3, 4, 5, k4) from baseall order by k1'
    runner.check(line)
    lind = 'select greatest(1, 2, 3, 4, 5, k1) from baseall order by k1'
    runner.check(line)
    lind = 'select greatest(1, 2, 3, 4, 5, k2) from baseall order by k1'
    runner.check(line)
    lind = 'select greatest(1, 2, 3, 4, 5, k3) from baseall order by k1'
    runner.check(line)
    line = 'select greatest(1.1, 1.2, 1.3, 1.4, 1.5, k5) from baseall order by k1'
    runner.check(line)
    line = 'select greatest(1.1, 1.2, 1.3, 1.4, 1.5, k8) from baseall order by k1'
    runner.check(line)
    line = 'select greatest("john", k6) from baseall order by k1'
    runner.check(line)
    line = 'select greatest("john", k7) from baseall order by k1'
    runner.check(line)
    line = 'select cast(greatest(cast("2010-10-11" as date), k10) as date) from baseall order by k1'
    runner.check(line)
    line = 'select cast(greatest(cast("2010-10-11" as datetime), k11) as datetime) from baseall \
            order by k1'
    runner.check(line)
    line = 'select least(k1, k2) from baseall order by k1'
    runner.check(line)
    line = 'select least(1, 2, 3, 4, 5, 6)'
    runner.check(line)
    line = 'select least(1.1, 1.2, 1.3, 1.4, 1.5, k9) from baseall order by k1'
    runner.check(line)
    lind = 'select least(1, 2, 3, 4, 5, k4) from baseall order by k1'
    runner.check(line)
    lind = 'select least(1, 2, 3, 4, 5, k1) from baseall order by k1'
    runner.check(line)
    lind = 'select least(1, 2, 3, 4, 5, k2) from baseall order by k1'
    runner.check(line)
    lind = 'select least(1, 2, 3, 4, 5, k3) from baseall order by k1'
    runner.check(line)
    line = 'select least(1.1, 1.2, 1.3, 1.4, 1.5, k5) from baseall order by k1'
    runner.check(line)
    line = 'select least(1.1, 1.2, 1.3, 1.4, 1.5, k8) from baseall order by k1'
    runner.check(line)
    line = 'select least("john", k6) from baseall order by k1'
    runner.check(line)
    line = 'select least("john", k7) from baseall order by k1'
    runner.check(line)
    line = 'select cast(least(cast("2010-10-11" as date), k10) as date) from baseall order by k1'
    runner.check(line)
    line = 'select cast(least(cast("2010-10-11" as datetime), k11) as datetime) from baseall \
            order by k1'
    runner.check(line)


def test_query_pmod():
    """
    {
    "title": "test_query_numeric_function.test_query_pmod",
    "describe": "pmod",
    "tag": "function,p0"
    }
    """
    """pmod"""
    line1 = 'select pmod(12, 3), pmod(12, 5), pmod(-12, 3), pmod(-12, 5)'
    line2 = 'select 0, 2, 0, 3'
    runner.check2(line1, line2)


def test_query_mod():
    """
    {
    "title": "test_query_numeric_function.test_query_mod",
    "describe": "test for mod & fmod",
    "tag": "function,p0"
    }
    """
    """
    test for mod & fmod
    """
    line = "select mod(abs(k2), 2), mod(k3, 6), mod(k5, -4), mod(abs(k8)*1000, 1.2), mod(-abs(k9)*10000, -0.5),\
        mod(k1, 0.02) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check(line)
    line2 = "select fmod(abs(k2), 2), fmod(k3, 6), fmod(k5, -4), fmod(abs(k8)*1000, 1.2), fmod(-abs(k9)*10000, -0.5),\
        fmod(k1, 0.02) from %s order by k1, k2, k3, k4" % (table_name)
    runner.check2(line2, line)


def test_topn():
    """
    {
    "title": "test_query_numeric_function.test_topn",
    "describe": "topn",
    "tag": "function,p0"
    }
    """
    line1 = 'select topn(k6, 1) from baseall'
    line2 = 'select \'{"false":8}\''
    runner.check2(line1, line2)


def test_query_bitand():
    """
    {
    "title": "test_query_numeric_function.test_query_bitand",
    "describe": "按位与运算",
    "tag": "function,p0"
    }
    """
    line1 = 'select BIN(bitand(k1, k3)) from %s order by k1,k2,k3,k4' % (table_name)
    line2 = 'select BIN(k1 & k3) from %s order by k1,k2,k3,k4' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitand(k2, k4)) from %s order by k1,k2,k3,k4' % (table_name)
    line2 = 'select BIN(k2 & k4) from %s order by k1,k2,k3,k4' % (table_name)
    runner.check2(line1, line2)


def test_query_bitnot():
    """
    {
    "title": "test_query_numeric_function.test_query_bitnot",
    "describe": "按位非运算",
    "tag": "function,p0"
    }
    """
    line1 = 'select BIN(bitnot(k1)) from %s order by k1' % (table_name)
    line2 = 'select BIN(~k1) from %s order by k1' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitnot(k2)) from %s order by k2' % (table_name)
    line2 = 'select BIN(~k2) from %s order by k2' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitnot(k3)) from %s order by k3' % (table_name)
    line2 = 'select BIN(~k3) from %s order by k3' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitnot(k4)) from %s order by k4' % (table_name)
    line2 = 'select BIN(~k4) from %s order by k4' % (table_name)
    runner.check2(line1, line2)


def test_query_bitor():
    """
    {
    "title": "test_query_numeric_function.test_query_bitor",
    "describe": "按位或运算",
    "tag": "function,p0"
    }
    """
    line1 = 'select BIN(bitor(k1, k3)) from %s order by k1,k3' % (table_name)
    line2 = 'select BIN(k1 | k3) from %s order by k1,k3' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitor(k2, k4)) from %s order by k2,k4' % (table_name)
    line2 = 'select BIN(k2 | k4) from %s order by k2,k4' % (table_name)
    runner.check2(line1, line2)


def test_query_bitxor():
    """
    {
    "title": "test_query_numeric_function.test_query_bitxor",
    "describe": "按位异或运算",
    "tag": "function,p0"
    }
    """
    line1 = 'select BIN(bitxor(k1, k3)) from %s order by k1,k3' % (table_name)
    line2 = 'select BIN(k1 ^ k3) from %s order by k1,k3' % (table_name)
    runner.check2(line1, line2)
    line1 = 'select BIN(bitxor(k2, k4)) from %s order by k2,k4' % (table_name)
    line2 = 'select BIN(k2 ^ k4) from %s order by k2,k4' % (table_name)
    runner.check2(line1, line2)


def teardown_module():
    """
    end 
    """
    print("End")
    # mysql_cursor.close()
    # mysql_con.close()
