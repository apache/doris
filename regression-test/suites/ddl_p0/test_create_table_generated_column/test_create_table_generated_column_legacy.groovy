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

suite("test_create_table_generated_column_legacy") {
    // test legacy planner create
    sql "SET enable_nereids_planner=false;"
    sql "drop table if exists test_gen_col_common_legacy"
    qt_common_default """create table test_gen_col_common_legacy(a int,b int,c double generated always as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    sql "drop table if exists test_gen_col_without_generated_always_legacy"
    qt_common_without_generated_always """create table test_gen_col_without_generated_always_legacy(a int,b int,c double as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    sql "drop table if exists test_gen_col_in_middle_legacy"
    qt_gencol_in_middle """create table test_gen_col_in_middle_legacy(a int,c double generated always as (abs(a+b)) not null,b int)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    sql "drop table if exists gencol_refer_gencol_legacy"
    qt_gencol_refer_gencol """
    create table gencol_refer_gencol_legacy(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    """
    sql "drop table if exists test_gen_col_array_func_legacy"
    qt_gencol_array_function_create """
    create table test_gen_col_array_func_legacy(pk int,a array<int>,b array<int>, c array<int> generated always as (array_union(a,b)) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
    ;
    """
    sql "drop table if exists test_gen_col_element_at_func_legacy"
    qt_gencol_array_function_element_at_create """
    create table test_gen_col_element_at_func_legacy(pk int,a array<int>,b array<int>, c int generated always as (element_at(a, 1)) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
    ;
    """
    test {
        sql """
        create table gencol_type_check(a int,b int, c array<int> generated always as (abs(a+b,3)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "In generated column 'c', no matching function with signature"
    }

    // gencol_has_sum
    test {
        sql """
        create table gencol_has_sum(a int,b int, c int generated always as (sum(a)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Expression of generated column 'c' contains a disallowed function"
    }

    // gencol_has_column_not_define
    test {
        sql """
        create table gencol_has_sum(a int,b int, c int generated always as (abs(d)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Unknown column 'd' in 'generated column function'"
    }

    // gencol_refer_gencol_after
    test {
        sql """
        create table gencol_refer_gencol_legacy(a int,c double generated always as (abs(a+d)) not null,b int, d int generated always as(c+1))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated column can refer only to generated columns defined prior to it."
    }

    sql "set @myvar=2"
    // gencol_has_var
    test {
        sql """
        create table test_gen_col_not_null100(a varchar(10),c double generated always as (abs(a+b+@myvar)) not null,b int)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated column expression cannot contain variable."
    }

    test {
        sql """
        create table test_gen_col_auto_increment(a bigint not null auto_increment, b int, c int as (a*b)) 
        distributed by hash(a) properties("replication_num" = "1");
        """
        exception "Generated column 'c' cannot refer to auto-increment column."
    }

    test{
        sql """
        create table test_gen_col_subquery(a int,b int, c int generated always as (a+(select 1)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated column does not support subquery."
    }

    test {
        sql """
        create table test_gen_col_array_func_lambda(pk int,a array<int>,b array<int>, c array<int> generated always as (array_count(x->(x%2=0),b)) not null)
        DISTRIBUTED BY HASH(pk)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated column does not support lambda."
    }

    test {
        sql """
            create table test_gen_col_array_func_legacy(pk int,a array<int>,b array<int>, c double generated always as (a+b) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
        """
        exception "In generated column 'c', can not cast from origin type"
    }

    test {
        sql """
        create table test_window_func(a int default 10, b int default 100, c boolean as(rank() over())) DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");"""
        exception "Expression of generated column 'c' contains a disallowed expression:'rank() OVER ()'"
    }
    test {
        sql """
        create table test_grouping(a int default 10, b int default 100, c boolean as(grouping(a)))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");"""
        exception "Expression of generated column 'c' contains a disallowed function:'grouping'"
    }

    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    qt_common_default_insert "INSERT INTO test_gen_col_common_legacy values(6,7,default);"
    qt_common_default_insert_with_specific_column "INSERT INTO test_gen_col_common_legacy(a,b) values(1,2);"
    qt_common_default_test_insert_default "INSERT INTO test_gen_col_common_legacy values(3,5,default);"
    qt_commont_default_select "select * from test_gen_col_common_legacy order by 1,2,3;"

    // qt_common_default_test_insert_null
    test {
        sql "INSERT INTO test_gen_col_common_legacy(a,b) values(1,null);"
        def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
        exception exception_str
    }

    // qt_common_default_test_insert_gencol
    test {
        sql "INSERT INTO test_gen_col_common_legacy values(1,2,3);"
        exception "The value specified for generated column 'c' in table 'test_gen_col_common_legacy' is not allowed."
    }


    qt_common_without_generated_always_insert "INSERT INTO test_gen_col_without_generated_always_legacy values(6,7,default);"
    qt_common_without_generated_always_insert_with_specific_column "INSERT INTO test_gen_col_without_generated_always_legacy(a,b) values(1,2);"
    qt_commont_without_generated_always_select "select * from test_gen_col_without_generated_always_legacy order by 1,2,3;"


    qt_gencol_in_middle_insert "insert into test_gen_col_in_middle_legacy values(1,default,5);"
    qt_gencol_in_middle_insert_with_specific_column "insert into test_gen_col_in_middle_legacy(a,b) values(4,5);"
    qt_gencol_in_middle_insert_with_specific_column_2 "insert into test_gen_col_in_middle_legacy(a,b,c) values(1,6,default);"
    qt_gencol_in_middle_select "select * from test_gen_col_in_middle_legacy order by 1,2,3;"


    qt_gencol_refer_gencol_insert "insert into gencol_refer_gencol_legacy values(1,default,5,default);"
    qt_gencol_refer_gencol_insert2 "insert into gencol_refer_gencol_legacy(a,b) values(5,6);"
    qt_gencol_refer_gencol_insert3 "insert into gencol_refer_gencol_legacy(a,b,c) values(2,9,default);"
    qt_gencol_refer_gencol_insert4 "insert into gencol_refer_gencol_legacy(a,b,c,d) values(3,3,default,default);"
    qt_gencol_refer_gencol_select "select * from gencol_refer_gencol_legacy order by 1,2,3,4;"


    qt_gencol_array_function_insert "insert into test_gen_col_array_func_legacy values(1,[1,2],[3,2],default);"
    qt_gencol_array_function_select "select * from test_gen_col_array_func_legacy"


    qt_gencol_array_function_element_at_insert "insert into test_gen_col_element_at_func_legacy values(1,[1,2],[3,2],default);"
    qt_gencol_array_function_element_at_select "select * from test_gen_col_element_at_func_legacy"

    test {
        sql """
       create table test_gen_col_aggregate(a int,b int,c int sum generated always as (abs(a+1)) not null)
        aggregate key(a,b)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "The generated columns can be key columns, or value columns of replace and replace_if_not_null aggregation type."
    }

    // test drop dependency
    sql "drop table if exists gencol_refer_gencol_legacy"
    qt_gencol_refer_gencol """
    create table gencol_refer_gencol_legacy(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    """
    sql "insert into gencol_refer_gencol_legacy(a,b) values(3,4)"
    test {
        sql "alter table gencol_refer_gencol_legacy drop column a"
        exception "Column 'a' has a generated column dependency on :[c]"
    }
    test {
        sql "alter table gencol_refer_gencol_legacy drop column c"
        exception "Column 'c' has a generated column dependency on :[d]"
    }
    sql "alter table gencol_refer_gencol_legacy drop column d"
    sql "alter table gencol_refer_gencol_legacy drop column c"
    sql "alter table gencol_refer_gencol_legacy drop column b"
    qt_test_drop_column "select * from gencol_refer_gencol_legacy"
    test {
        sql """
        create table test_gen_col_default(a int,b int,c int  generated always as (abs(a+1)) not null default 10)
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated columns cannot have default value."
    }
    test {
        sql """
        create table test_gen_col_increment(a int,b int,c int  generated always as (abs(a+1)) not null auto_increment)
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated columns cannot be auto_increment."
    }

}