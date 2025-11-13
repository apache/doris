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

suite("test_generated_column") {
    // ========== 测试1: 乘法运算 ==========
    // 打开enable_decimal256建表
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_multi_decimalv3;
        create table t_gen_col_multi_decimalv3(a decimal(20,5),b decimal(21,6),c decimal(38,11) generated always as (a*b) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    // 关闭enable_decimal256，插入数据
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_multi_decimalv3 values(1.12343,1.123457,default);"

    // 查询数据,预期column c的scale为11
    qt_c_scale_is_11 "select * from t_gen_col_multi_decimalv3;"

    // ========== 测试4: 除法运算 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_divide_decimalv3;
        create table t_gen_col_divide_decimalv3(a decimal(38,18),b decimal(38,18),c decimal(38,18) generated always as (a/b) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_divide_decimalv3 values(100.123456789012345678,2.123456789012345678,default);"
    qt_divide_scale "select * from t_gen_col_divide_decimalv3;"

    // ========== 测试2: 加法运算 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_add_sub_mod_decimalv3;
        create table t_gen_col_add_sub_mod_decimalv3(a decimal(38,9),b decimal(38,10),c decimal(38,10) generated always as (a+b) not null, d decimal(38,10) generated always as (a-b) not null,
        f decimal(38,10) generated always as (mod(b,a)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_add_sub_mod_decimalv3 values(1.012345678,1.0123456789,default,default,default);"
    qt_add_sub_mod "select * from t_gen_col_add_sub_mod_decimalv3;"


    // ========== 测试7: 嵌套生成列（生成列引用其他生成列） ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_nested;
        create table t_gen_col_nested(
            a decimal(20,5),
            b decimal(21,6),
            c decimal(38,11) generated always as (a*b) not null,
            d decimal(38,11) generated always as (c+1) not null
        )
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_nested values(1.12343,1.123457,default,default);"
    qt_nested_cols "select * from t_gen_col_nested;"

    // ========== 测试8: 复杂表达式组合 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_complex;
        create table t_gen_col_complex(
            a decimal(20,5),
            b decimal(21,6),
            c decimal(38,11) generated always as (a*b) not null,
            d decimal(38,11) generated always as (a*b+a) not null,
            e decimal(38,11) generated always as (a*b-b) not null
        )
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_complex values(1.12343,1.123457,default,default,default);"
    qt_complex_expr "select * from t_gen_col_complex;"

    // ========== 测试18: 生成列在CASE WHEN中使用 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_case;
        create table t_gen_col_case(
            a decimal(20,5),
            b decimal(21,6),
            c decimal(38,11) generated always as (a*b) not null,
            d decimal(38,11) generated always as (case when a > 1 then a*b else a end) not null
        )
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_case values(1.12343,1.123457,default,default);"
    qt_gen_col_case "select * from t_gen_col_case;"

    // ========== 测试19: 生成列在IF函数中使用 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_if;
        create table t_gen_col_if(
            a decimal(20,5),
            b decimal(21,6),
            c decimal(38,11) generated always as (if(a > 1, a*b, a)) not null
        )
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_if values(1.12343,1.123457,default);"
    qt_gen_col_if "select * from t_gen_col_if;"

    // ========== 测试20: 生成列在COALESCE/GREATEST/LEAST等函数中使用 ==========
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_funcs;
        create table t_gen_col_funcs(
            a decimal(20,5),
            b decimal(21,6),
            c decimal(38,11) generated always as (a*b) not null,
            d decimal(38,11) generated always as (greatest(a*b, a)) not null,
            e decimal(38,11) generated always as (least(a*b, b)) not null
        )
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_funcs values(1.12343,1.123457,default,default,default);"
    qt_gen_col_funcs "select * from t_gen_col_funcs;"

}