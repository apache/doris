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

suite("test_generated_column_fault_tolerance_nereids") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    test {
        sql """
        create table gencol_type_check(a int,b int, c array<int> generated always as (abs(a+b,3)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "In generated column 'c', can not found function 'abs' which has 2 arity."
    }

    // gencol_has_sum
    test {
        sql """
        create table gencol_has_sum(a int,b int, c int generated always as (sum(a)) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Expression of generated column 'c' contains a disallowed function:'sum'"
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
        create table gencol_refer_gencol_ft(a int,c double generated always as (abs(a+d)) not null,b int, d int generated always as(c+1))
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
            create table test_gen_col_array_func(pk int,a array<int>,b array<int>, c double generated always as (a+b) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
        """
        exception "In generated column 'c', cannot cast from ARRAY<INT> to numeric type"
    }

    test {
        sql """
        create table test_gen_col_aggregate_value(a int,b int,c int sum generated always as (abs(a+1)) not null)
        aggregate key(a,b)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "The generated columns can be key columns, or value columns of replace and replace_if_not_null aggregation type."
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
       create table test_gen_col_on_update(a int,b int,c datetimev2  generated always as (CURRENT_TIMESTAMP) not null default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP )
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
        exception "Generated columns cannot have default value."
    }
    test {
        sql """
        create table test_window_func(a int default 10, b int default 100, c boolean as(rank() over())) DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");"""
        exception "Expression of generated column 'c' contains a disallowed function:'rank'"
    }

    test {
        sql """
        create table test_grouping(a int default 10, b int default 100, c boolean as(grouping(a)))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");"""
        exception "Expression of generated column 'c' contains a disallowed function:'Grouping'"
    }

    sql "drop table if exists gen_col_test_modify"
    sql """ create table gen_col_test_modify(pk int, a int, b int, c int as (a+b)) distributed by hash(pk) buckets 10
             properties('replication_num' = '1'); """
    test {
        sql """ALTER TABLE gen_col_test_modify modify COLUMN c int AS (a+b+1)"""
        exception "Not supporting alter table modify generated columns."
    }
    test {
        sql """ALTER TABLE gen_col_test_modify ADD COLUMN d int AS (a+b);"""
        exception "Not supporting alter table add generated columns."
    }

    sql "drop table if exists test_gen_col_common_ft"
    sql """create table test_gen_col_common_ft(a int,b int,c double generated always as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    // qt_common_default_test_insert_null
    def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
    test {
        sql "INSERT INTO test_gen_col_common_ft(a,b) values(1,null);"
        exception exception_str
    }

    // qt_common_default_test_insert_gencol
    test {
        sql "INSERT INTO test_gen_col_common_ft values(1,2,3);"
        exception "The value specified for generated column 'c' in table 'test_gen_col_common_ft' is not allowed."
    }
    test {
        sql "INSERT INTO test_gen_col_common_ft(a,b,c) values(1,2,3);"
        exception "The value specified for generated column 'c' in table 'test_gen_col_common_ft' is not allowed."
    }
    test {
        sql "INSERT INTO test_gen_col_common_ft select 1,2,5"
        exception "The value specified for generated column 'c' in table 'test_gen_col_common_ft' is not allowed."
    }
    test {
        sql "INSERT INTO test_gen_col_common_ft(a,b,c) select * from test_gen_col_common_ft"
        exception "The value specified for generated column 'c' in table 'test_gen_col_common_ft' is not allowed."
    }


}