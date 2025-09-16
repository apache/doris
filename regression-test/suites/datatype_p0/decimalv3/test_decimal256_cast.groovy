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

suite("test_decimal256_cast") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"
    sql """
        set debug_skip_fold_constant=true;
    """

    qt_decimal256_cast0 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast1 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast(-999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast2 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast3 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast(-999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""

    qt_decimal256_cast4 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast("999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast5 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast("-999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast6 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast("999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast7 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast("-999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""

    qt_decimal256_cast8 """
        select cast('0.000000000000000000000000000000000000000000000000000000000000000000000012345678901' as decimalv3(76,0));
    """

    sql """
        drop table  if exists cast_to_dec256;
    """
    sql """
    create table cast_to_dec256 (
        k1 int,
        v1 varchar(128)
    ) distributed by hash(k1)
    properties (
        'replication_num' = '1'
    );
    """
    sql """
        insert into cast_to_dec256  values(9, "999999999999999999999999999999999999999999999999999999999999999999.9999999999"),
            (-9, "-999999999999999999999999999999999999999999999999999999999999999999.9999999999");
    """
    qt_decimal256_cast9 """
        select k1, cast(v1 as decimalv3(76,10)) from cast_to_dec256 order by k1, v1;
    """

    sql """
        truncate table cast_to_dec256;
    """
    sql """
        insert into cast_to_dec256  values(10, "0.000000000000000000000000000000000000000000000000000000000000000000000012345678901");
    """
    qt_decimal256_cast10 """
        select k1, cast(v1 as decimalv3(76, 0)) from cast_to_dec256 order by k1, v1;
    """

     qt_decimal256_cast_to_float1 """
         select /*+SET_VAR(enable_fold_constant_by_be = true) */cast(cast("12345678.000000000000000000000000000000001" as decimalv3(76, 60)) as float);
     """
     qt_decimal256_cast_to_float2 """
         select /*+SET_VAR(enable_fold_constant_by_be = false) */cast(cast("12345678.000000000000000000000000000000001" as decimalv3(76, 60)) as float);
     """

    sql """
        drop table  if exists dec256cast_to_float;
    """
    sql """
    create table dec256cast_to_float (
        k1 int,
        v1 decimalv3(76, 60)
    ) distributed by hash(k1)
    properties (
        'replication_num' = '1'
    );
    """
    sql """
        insert into dec256cast_to_float values 
            (1, "12345678.000000000000000000000000000000001"),
            (2, "9999999999999999.999999999999999999999999999999999999999999999999999999999999"),
            (3, "0.000000250794773934844880991039000000000000000000000000000000"),
            (4, "0.999999999999999999999999999999999999999999999999999999999999");
    """
    
    qt_decimal256_cast_to_float3 """
        select k1, cast(v1 as float) from dec256cast_to_float order by 1;
    """
    qt_decimal256_cast_to_double_1 """
        select k1, cast(v1 as double) from dec256cast_to_float order by 1;
    """

    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("1000000000000000000000000000000000000000000000000000000000000000000000.111111" as decimalv3(76, 6)) as float);"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("1000000000000000000000000000000000000000000000000000000000000000000000.111111" as decimalv3(76, 6)) as float);"""
        exception "Arithmetic overflow"
    }

    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("9999999999999999999999999999999999999999999999999999999999999999999999.999999" as decimalv3(76, 6)) as float);"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("9999999999999999999999999999999999999999999999999999999999999999999999.999999" as decimalv3(76, 6)) as float);"""
        exception "Arithmetic overflow"
    }

    sql "drop table if exists dec256cast_to_float_overflow"
    sql """
    create table dec256cast_to_float_overflow (
        k1 int,
        v1 decimalv3(76, 6)
    ) distributed by hash(k1)
    properties (
        'replication_num' = '1'
    );
    """
    sql """
        insert into dec256cast_to_float_overflow values  (1, "1000000000000000000000000000000000000000000000000000000000000000000000.111111");
    """
    test {
        sql "select cast(v1 as float) from dec256cast_to_float_overflow;"
        exception "Arithmetic overflow"
    }
    sql """
        truncate table dec256cast_to_float_overflow;
    """
    sql """
        insert into dec256cast_to_float_overflow values  (1, "9999999999999999999999999999999999999999999999999999999999999999999999.999999");
    """
    test {
        sql "select cast(v1 as float) from dec256cast_to_float_overflow;"
        exception "Arithmetic overflow"
    }

    // cast float to decimal256
    qt_cast_float_to_decimal256_const_1_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("219.77718" as float) as decimalv3(76, 38));"""
    qt_cast_float_to_decimal256_const_1_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("219.77718" as float) as decimalv3(76, 38));"""

    qt_cast_float_to_decimal256_const_2_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-219.77718" as float) as decimalv3(76, 38));"""
    qt_cast_float_to_decimal256_const_2_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-219.77718" as float) as decimalv3(76, 38));"""

    qt_cast_float_to_decimal256_const_3_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999999" as float) as decimalv3(76, 38));"""
    qt_cast_float_to_decimal256_const_3_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999999" as float) as decimalv3(76, 38));"""

    qt_cast_float_to_decimal256_const_4_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999999" as float) as decimalv3(76, 38));"""
    qt_cast_float_to_decimal256_const_4_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999999" as float) as decimalv3(76, 38));"""

    qt_cast_float_to_decimal256_const_5_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("9999999999999999999999999999998.999999999999999999999999999999999999999999999" as float) as decimalv3(76, 45));"""
    qt_cast_float_to_decimal256_const_5_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("9999999999999999999999999999998.999999999999999999999999999999999999999999999" as float) as decimalv3(76, 45));"""

    qt_cast_float_to_decimal256_const_6_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-9999999999999999999999999999998.999999999999999999999999999999999999999999999" as float) as decimalv3(76, 45));"""
    qt_cast_float_to_decimal256_const_6_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-9999999999999999999999999999998.999999999999999999999999999999999999999999999" as float) as decimalv3(76, 45));"""

    qt_cast_float_to_decimal256_const_7_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("0.8999999999999999999999999999999999999999999999999999999999999999999999999999" as float) as decimalv3(76, 76));"""
    qt_cast_float_to_decimal256_const_7_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("0.8999999999999999999999999999999999999999999999999999999999999999999999999999" as float) as decimalv3(76, 76));"""

    qt_cast_float_to_decimal256_const_8_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-0.8999999999999999999999999999999999999999999999999999999999999999999999999999" as float) as decimalv3(76, 76));"""
    qt_cast_float_to_decimal256_const_8_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-0.8999999999999999999999999999999999999999999999999999999999999999999999999999" as float) as decimalv3(76, 76));"""

    qt_cast_float_to_decimal256_const_9_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("3.402e38" as float) as decimalv3(76, 37));"""
    qt_cast_float_to_decimal256_const_9_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("3.402e38" as float) as decimalv3(76, 37));"""
    qt_cast_float_to_decimal256_const_10_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-3.402e38" as float) as decimalv3(76, 37));"""
    qt_cast_float_to_decimal256_const_10_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-3.402e38" as float) as decimalv3(76, 37));"""

    sql """
        drop table if exists cast_to_float_to_decimal256;
    """
    sql """
    create table cast_to_float_to_decimal256 (
        k1 int,
        v1 float
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_float_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (2, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (3, "-99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "-0.97718"),
            (10, "3.402e38"),
            (11, "-3.402e38");
    """
    qt_cast_float_to_decimal256_1 """
        select k1, cast(v1 as decimalv3(76, 0)) from cast_to_float_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_float_to_decimal256;
    """
    sql """
    create table cast_to_float_to_decimal256 (
        k1 int,
        v1 float
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_float_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (2, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (3, "-99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "-0.97718");
    """
    qt_cast_float_to_decimal256_2 """
        select k1, cast(v1 as decimalv3(76, 38)) from cast_to_float_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_float_to_decimal256;
    """
    sql """
    create table cast_to_float_to_decimal256 (
        k1 int,
        v1 float
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_float_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "0.97718");
    """
    qt_cast_float_to_decimal256_3 """
        select k1, cast(v1 as decimalv3(76, 45)) from cast_to_float_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_float_to_decimal256;
    """
    sql """
    create table cast_to_float_to_decimal256 (
        k1 int,
        v1 float
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_float_to_decimal256 values 
            (6, "0.8999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.8999999999999999999999999999999999999999999999999999999999999999999999999999");
    """
    qt_cast_float_to_decimal256_4 """
        select k1, cast(v1 as decimalv3(76, 76)) from cast_to_float_to_decimal256 order by k1;
    """

    // cast double to decimal256
    qt_cast_double_to_decimal256_const_1_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("219.77718" as double) as decimalv3(76, 38));"""
    qt_cast_double_to_decimal256_const_1_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("219.77718" as double) as decimalv3(76, 38));"""

    qt_cast_double_to_decimal256_const_2_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-219.77718" as double) as decimalv3(76, 38));"""
    qt_cast_double_to_decimal256_const_2_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-219.77718" as double) as decimalv3(76, 38));"""

    qt_cast_double_to_decimal256_const_3_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999999" as double) as decimalv3(76, 38));"""
    qt_cast_double_to_decimal256_const_3_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999999" as double) as decimalv3(76, 38));"""

    qt_cast_double_to_decimal256_const_4_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999999" as double) as decimalv3(76, 38));"""
    qt_cast_double_to_decimal256_const_4_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999999" as double) as decimalv3(76, 38));"""

    qt_cast_double_to_decimal256_const_5_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("9999999999999999999999999999998.999999999999999999999999999999999999999999999" as double) as decimalv3(76, 45));"""
    qt_cast_double_to_decimal256_const_5_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("9999999999999999999999999999998.999999999999999999999999999999999999999999999" as double) as decimalv3(76, 45));"""

    qt_cast_double_to_decimal256_const_6_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-9999999999999999999999999999998.999999999999999999999999999999999999999999999" as double) as decimalv3(76, 45));"""
    qt_cast_double_to_decimal256_const_6_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-9999999999999999999999999999998.999999999999999999999999999999999999999999999" as double) as decimalv3(76, 45));"""

    qt_cast_double_to_decimal256_const_7_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("0.9899999999999999999999999999999999999999999999999999999999999999999999999999" as double) as decimalv3(76, 76));"""
    qt_cast_double_to_decimal256_const_7_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("0.9899999999999999999999999999999999999999999999999999999999999999999999999999" as double) as decimalv3(76, 76));"""

    qt_cast_double_to_decimal256_const_8_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-0.9899999999999999999999999999999999999999999999999999999999999999999999999999" as double) as decimalv3(76, 76));"""
    qt_cast_double_to_decimal256_const_8_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-0.9899999999999999999999999999999999999999999999999999999999999999999999999999" as double) as decimalv3(76, 76));"""

    qt_cast_double_to_decimal256_const_9_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("9.899e75" as double) as decimalv3(76, 0));"""
    qt_cast_double_to_decimal256_const_9_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("9.899e75" as double) as decimalv3(76, 0));"""
    qt_cast_double_to_decimal256_const_10_1 """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-9.899e75" as double) as decimalv3(76, 0));"""
    qt_cast_double_to_decimal256_const_10_2 """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-9.899e75" as double) as decimalv3(76, 0));"""

    sql """
        drop table if exists cast_to_double_to_decimal256;
    """
    sql """
    create table cast_to_double_to_decimal256 (
        k1 int,
        v1 double
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_double_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (2, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (3, "-99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "-0.97718"),
            (10, "3.402e38"),
            (11, "-3.402e38"),
            (12, "9.899e75"),
            (13, "9.899e75");
    """
    qt_cast_double_to_decimal256_1 """
        select k1, cast(v1 as decimalv3(76, 0)) from cast_to_double_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_double_to_decimal256;
    """
    sql """
    create table cast_to_double_to_decimal256 (
        k1 int,
        v1 double
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_double_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (2, "99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (3, "-99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "-0.97718");
    """
    qt_cast_double_to_decimal256_2 """
        select k1, cast(v1 as decimalv3(76, 38)) from cast_to_double_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_double_to_decimal256;
    """
    sql """
    create table cast_to_double_to_decimal256 (
        k1 int,
        v1 double
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_double_to_decimal256 values 
            (0, "219.77718"), (1, "-219.77718"),
            (4, "9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (5, "-9999999999999999999999999999998.999999999999999999999999999999999999999999999"),
            (6, "0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (8, "0.97718"),
            (9, "0.97718");
    """
    qt_cast_double_to_decimal256_3 """
        select k1, cast(v1 as decimalv3(76, 45)) from cast_to_double_to_decimal256 order by k1;
    """

    sql """
        drop table if exists cast_to_double_to_decimal256;
    """
    sql """
    create table cast_to_double_to_decimal256 (
        k1 int,
        v1 double
    ) properties ('replication_num' = '1');
    """
    sql """
        insert into cast_to_double_to_decimal256 values 
            (6, "0.8999999999999999999999999999999999999999999999999999999999999999999999999999"),
            (7, "-0.8999999999999999999999999999999999999999999999999999999999999999999999999999");
    """
    qt_cast_double_to_decimal256_4 """
        select k1, cast(v1 as decimalv3(76, 76)) from cast_to_double_to_decimal256 order by k1;
    """

    // test cast float to decimal256 overflow
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("219.77718" as float) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("219.77718" as float) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-219.77718" as float) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-219.77718" as float) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }

    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("3.402e38" as float) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("3.402e38" as float) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-3.402e38" as float) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-3.402e38" as float) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }

    // test cast double to decimal256 overflow
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("219.77718" as double) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("219.77718" as double) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-219.77718" as double) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-219.77718" as double) as decimalv3(76, 74));"""
        exception "Arithmetic overflow"
    }

    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("3.402e38" as double) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("3.402e38" as double) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-3.402e38" as double) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-3.402e38" as double) as decimalv3(76, 38));"""
        exception "Arithmetic overflow"
    }

    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("9.899e76" as double) as decimalv3(76, 0));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("9.899e76" as double) as decimalv3(76, 0));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = true) */cast(cast("-9.899e76" as double) as decimalv3(76, 0));"""
        exception "Arithmetic overflow"
    }
    test {
        sql """select /*+SET_VAR(debug_skip_fold_constant = false) */cast(cast("-9.899e76" as double) as decimalv3(76, 0));"""
        exception "Arithmetic overflow"
    }
}