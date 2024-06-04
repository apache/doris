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

suite("test_bit_shift") {
    qt_comment """
        select "testing big_shift_left";
    """
    def INT64_MAX=9223372036854775807;
    def INT64_MIN=-9223372036854775808;
    def INT8_MAX=127
    def INT8_MIN=-128

    // bit_shift_left
    
    qt_numbers 'select bit_shift_left(number, 4) from numbers("number"="10");'
    qt_0 "select bit_shift_left(0, 0);"
    qt_1 "select bit_shift_left(1, 10);"
    qt_2 "select bit_shift_left(1, 63);"
    qt_3 "select bit_shift_left(1, 64);"
    qt_select "select bit_shift_left(127, 1);"
    qt_select "select bit_shift_left(cast (127 as TINYINT), 1);"
    qt_int64_max """
            WITH tbl AS (
                SELECT ${INT64_MAX} AS BIGINT_MAX)
            SELECT BIGINT_MAX, bit_shift_left(BIGINT_MAX, 1)
            FROM tbl
    """
    qt_int64_min """
            WITH tbl AS (
                SELECT ${INT64_MIN} AS BIGINT_MIN)
            SELECT BIGINT_MIN, bit_shift_left(BIGINT_MIN, 1)
            FROM tbl
    """
    qt_select """
            WITH tbl AS (
                SELECT ${INT8_MAX} AS TINYINT_MAX)
            SELECT TINYINT_MAX, bit_shift_left(1, TINYINT_MAX)
            FROM tbl
    """
    qt_select """
            WITH tbl AS (
                SELECT ${INT8_MIN} AS TINYINT_MIN)
            SELECT TINYINT_MIN, bit_shift_left(1, TINYINT_MIN)
            FROM tbl
    """

    sql """ drop table if exists test_bit_shift_nereids; """
    sql """
        create table test_bit_shift_nereids (
            rid tinyint,
            ti tinyint,
            si smallint,
            i int,
            bi bigint
        ) distributed by HASH(rid)
        properties("replication_num"="1");
    """
    sql """ insert into test_bit_shift_nereids values
            (0, 127, 32767, 2147483647, ${INT64_MAX}),
            (1, -128, -32768, -2147483648, ${INT64_MAX})"""

    qt_select """
            select rid, 
                bit_shift_left(ti, 1), bit_shift_left(si, 1),
                bit_shift_left(i, 1), bit_shift_left(bi, 1) 
                from test_bit_shift_nereids order by rid; """

    // bit_shift_right

    qt_comment """
        select "testing big_shift_right";
    """

    qt_select "SELECT bit_shift_right(0, 0);"
    qt_select "SELECT bit_shift_right(0, 127);"
    qt_select "SELECT bit_shift_right(${INT64_MAX}, 62);"
    qt_select "SELECT bit_shift_right(${INT64_MAX}, 63);"
    qt_select "SELECT bit_shift_right(${INT64_MAX}, 64);"
    qt_select "SELECT bit_shift_right(${INT64_MIN}, 62);"
    qt_select "SELECT bit_shift_right(${INT64_MIN}, 63);"
    qt_select "SELECT bit_shift_right(${INT64_MIN}, 64);"
    qt_select "SELECT bit_shift_right(1, ${INT8_MAX});"
    qt_select "SELECT bit_shift_right(1, ${INT8_MIN});"

    // nagitave shift count

    qt_select """SELECT bit_shift_right(1, -number) from numbers("number"="129") order by number;"""
    qt_select """SELECT bit_shift_right(1, -number) from numbers("number"="129") order by number;"""
}
