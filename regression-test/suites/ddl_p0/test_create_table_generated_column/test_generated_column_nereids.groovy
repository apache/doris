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
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    String db = context.config.getDbNameByFile(context.file)

    sql "drop table if exists test_gen_col_common"
    qt_common_default """create table test_gen_col_common(a int,b int,c double generated always as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    qt_common_default_insert "INSERT INTO test_gen_col_common values(6,7,default);"
    qt_common_default_insert_with_specific_column "INSERT INTO test_gen_col_common(a,b) values(1,2);"
    qt_common_default_test_insert_default "INSERT INTO test_gen_col_common values(3,5,default);"

    qt_commont_default_select  "select * from test_gen_col_common order by 1,2,3;"

    sql "drop table if exists test_gen_col_without_generated_always"
    qt_common_without_generated_always """create table test_gen_col_without_generated_always(a int,b int,c double as (abs(a+b)) not null)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    qt_common_without_generated_always_insert "INSERT INTO test_gen_col_without_generated_always values(6,7,default);"
    qt_common_without_generated_always_insert_with_specific_column "INSERT INTO test_gen_col_without_generated_always(a,b) values(1,2);"

    qt_commont_without_generated_always_select  "select * from test_gen_col_without_generated_always order by 1,2,3;"

    sql "drop table if exists test_gen_col_in_middle"
    qt_gencol_in_middle """create table test_gen_col_in_middle(a int,c double generated always as (abs(a+b)) not null,b int)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    qt_gencol_in_middle_insert "insert into test_gen_col_in_middle values(1,default,5);"
    qt_gencol_in_middle_insert_with_specific_column "insert into test_gen_col_in_middle(a,b) values(4,5);"
    qt_gencol_in_middle_insert_with_specific_column_2 "insert into test_gen_col_in_middle(a,b,c) values(1,6,default);"
    qt_gencol_in_middle_insert_multi_values "insert into test_gen_col_in_middle values(1,default,2),(3,default,4);"
    qt_gencol_in_middle_select "select * from test_gen_col_in_middle order by 1,2,3;"

    sql "drop table if exists test_gen_col_empty_values"
    qt_gencol_empty_values """create table test_gen_col_empty_values(a int default 10,c double generated always as (abs(a+b)) not null,b int default 100)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    qt_gencol_empty_values_insert "insert into test_gen_col_empty_values values(),()"
    qt_gencol_empty_values_insert_2 "insert into test_gen_col_empty_values(c) values(default),(default)"
    qt_gencol_empty_values_insert_3 "insert into test_gen_col_empty_values(a,c) values(1,default),(3,default)"
    qt_gencol_empty_values_insert_4 "insert into test_gen_col_empty_values(c,a) values(default,5),(default,6)"

    sql "drop table if exists gencol_refer_gencol"
    qt_gencol_refer_gencol """
    create table gencol_refer_gencol(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    """
    qt_gencol_refer_gencol_insert "insert into gencol_refer_gencol values(1,default,5,default);"
    qt_gencol_refer_gencol_insert2 "insert into gencol_refer_gencol(a,b) values(5,6);"
    qt_gencol_refer_gencol_insert3 "insert into gencol_refer_gencol(a,b,c) values(2,9,default);"
    qt_gencol_refer_gencol_insert4 "insert into gencol_refer_gencol(a,b,c,d) values(3,3,default,default);"
    qt_gencol_refer_gencol_insert_multi_values "insert into gencol_refer_gencol(a,b,c,d) values(3,3,default,default),(5,7,default,default);"
    qt_gencol_refer_gencol_select "select * from gencol_refer_gencol order by 1,2,3,4;"

    sql "drop materialized view if exists test_mv_gen_col on gencol_refer_gencol"
    createMV ("""create materialized view test_mv_gen_col as select a,sum(a),sum(c) ,sum(d) from gencol_refer_gencol group by a;""")
    qt_test_insert_mv "insert into gencol_refer_gencol(a,b) values(1,2),(3,5)"
    qt_test_select "select a,sum(a),sum(c),sum(d) from gencol_refer_gencol group by a order by 1,2,3,4"
    explain{
        sql "select a,sum(a),sum(c),sum(d) from gencol_refer_gencol group by a"
        contains "test_mv_gen_col"
    }

    sql "drop table if exists test_gen_col_array_func"
    qt_gencol_array_function_create """
    create table test_gen_col_array_func(pk int,a array<int>,b array<int>, c array<int> generated always as (array_union(a,b)) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
    ;
    """
    qt_gencol_array_function_insert "insert into test_gen_col_array_func values(1,[1,2],[3,2],default);"
    qt_gencol_array_function_select "select * from test_gen_col_array_func"

    sql "drop table if exists test_gen_col_element_at_func"
    qt_gencol_array_function_element_at_create """
    create table test_gen_col_element_at_func(pk int,a array<int>,b array<int>, c int generated always as (element_at(a, 1)) not null)
            DISTRIBUTED BY HASH(pk)
            PROPERTIES("replication_num" = "1");
    ;
    """
    qt_gencol_array_function_element_at_insert "insert into test_gen_col_element_at_func values(1,[1,2],[3,2],default);"
    qt_gencol_array_function_element_at_select "select * from test_gen_col_element_at_func"

    sql "drop table if exists gencol_like_t;"
    qt_create_table_like "create table gencol_like_t like test_gen_col_common;"
    qt_create_table_like_insert "insert into gencol_like_t values(1,2,default);"
    qt_create_table_like_select "select * from gencol_like_t"
    sql "drop table if exists gencol_like_t;"
    qt_refer_gencol_create_table_like "create table gencol_like_t like gencol_refer_gencol;"
    qt_refer_gencol_create_table_like_insert "insert into gencol_like_t values(1,default,2,default);"
    qt_refer_gencol_create_table_like_select "select * from gencol_like_t"

    sql "drop table if exists gencol_refer_gencol_row_store"
    qt_test_row_store """create table gencol_refer_gencol_row_store(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1","store_row_column" = "true");"""
    qt_row_store_insert "insert into gencol_refer_gencol_row_store values(1,default,5,default);"
    qt_row_store_select "select * from gencol_refer_gencol_row_store"

    sql "drop ENCRYPTKEY if exists my_key"
    sql """CREATE ENCRYPTKEY my_key AS "ABCD123456789";"""
    sql "drop table if exists test_encrypt_key_gen_col"
    qt_test_aes_encrypt """create table test_encrypt_key_gen_col(a int default 10, b int default 100,
    c varchar(100) as(hex(aes_encrypt("abc",key my_key))))  distributed by hash(a) properties("replication_num"="1");;"""
    qt_test_aes_encrypt_insert "insert into test_encrypt_key_gen_col values(1,2,default)"
    qt_test_aes_encrypt_select "select * from test_encrypt_key_gen_col"

    sql "drop table if exists test_encrypt_key_gen_col_with_db"
    qt_test_aes_encrypt_with_db """create table test_encrypt_key_gen_col_with_db(a int default 10, b int default 100,
    c varchar(100) as(hex(aes_encrypt("abc",key ${db}.my_key)))) distributed by hash(a) properties("replication_num"="1");"""
    qt_test_aes_encrypt_insert_with_db "insert into test_encrypt_key_gen_col_with_db values(1,2,default)"
    qt_test_aes_encrypt_select_with_db "select * from test_encrypt_key_gen_col_with_db"

    qt_describe "describe gencol_refer_gencol"

    //test update
    sql "drop table if exists test_gen_col_update"
    sql """create table test_gen_col_update (a int, b int, c int as (a+b))
    unique key(a)
    distributed by hash(a) properties("replication_num"="1")"""
    sql "insert into test_gen_col_update values(1,3,default)"
    qt_test_update "update test_gen_col_update set b=20"
    qt_test_update_generated_column "select * from test_gen_col_update"

    // test unique table, generated column is not key
    sql "drop table if exists test_gen_col_unique_key"
    qt_gen_col_unique_key """create table test_gen_col_unique_key(a int,b int,c int generated always as (abs(a+b)) not null)
    unique key(a,b)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    ;"""
    qt_gen_col_unique_key_insert "INSERT INTO test_gen_col_unique_key values(6,7,default);"
    qt_gen_col_unique_key_insert "INSERT INTO test_gen_col_unique_key values(6,7,default);"
    qt_gen_col_unique_key_select  "select * from test_gen_col_unique_key order by 1,2,3;"

    // test unique table,generated column is key
    sql "drop table if exists test_gen_col_unique_key2"
    sql """create table test_gen_col_unique_key2(a int,c int generated always as (abs(a+1)) not null,b int)
    unique key(a,c)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""
    qt_gen_col_unique_key_insert_is_key "INSERT INTO test_gen_col_unique_key2 values(6,default,7);"
    qt_gen_col_unique_key_insert_is_key "INSERT INTO test_gen_col_unique_key2 values(6,default,7);"
    qt_gen_col_unique_key_select_is_key "select * from test_gen_col_unique_key2"

    // test aggregate table,generated column is key
    sql "drop table if exists test_gen_col_aggregate"
    sql """
       create table test_gen_col_aggregate(a int,b int,c int  generated always as (abs(a+1)) not null)
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        """
    qt_gen_col_aggregate_insert "INSERT INTO test_gen_col_aggregate values(6,7,default);"
    qt_gen_col_aggregate_insert "INSERT INTO test_gen_col_aggregate values(6,7,default);"
    qt_gen_col_aggregate_select "select * from test_gen_col_aggregate"

    // test drop dependency
    sql "drop table if exists gencol_refer_gencol"
    qt_gencol_refer_gencol """
    create table gencol_refer_gencol(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");
    """
    sql "insert into gencol_refer_gencol(a,b) values(3,4)"
    test {
        sql "alter table gencol_refer_gencol drop column a"
        exception "Column 'a' has a generated column dependency on :[c]"
    }
    test {
        sql "alter table gencol_refer_gencol drop column c"
        exception "Column 'c' has a generated column dependency on :[d]"
    }
    sql "alter table gencol_refer_gencol drop column d"
    sql "alter table gencol_refer_gencol drop column c"
    sql "alter table gencol_refer_gencol drop column b"
    qt_test_drop_column "select * from gencol_refer_gencol"

    // test agg table, gen col is the key or replace/replace_if_not_null
    multi_sql """
        drop table if exists agg_gen_col_key;
        create table agg_gen_col_key(a int,b int, c int as (a+b), d int sum)
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into agg_gen_col_key(a,b) values(1,2);
        insert into agg_gen_col_key(a,b,d) values(1,2,6);
    """
    qt_test_agg_key "select * from agg_gen_col_key order by 1,2,3,4"

    sql "drop table if exists agg_gen_col_replace"
    qt_agg_gen_col_replace """create table agg_gen_col_replace(a int,b int, c int as (a+b), d int replace as (c+1))
    aggregate key(a,b,c)
    DISTRIBUTED BY HASH(a)
    PROPERTIES("replication_num" = "1");"""

    multi_sql """
        drop table if exists agg_gen_col_replace_if_not_null;
        create table agg_gen_col_replace_if_not_null(a int,b int, c int as (a+b), d int REPLACE_IF_NOT_NULL as (c+1), f int sum default 10)
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into agg_gen_col_replace_if_not_null(a,b,f) values(1,2,3);
        insert into agg_gen_col_replace_if_not_null(a,b) values(1,2);
    """
    qt_agg_replace_null "select * from agg_gen_col_replace_if_not_null order by 1,2,3,4"

    test {
        sql """create table agg_gen_col_replace(a int,b int, c int as (a+b), d int sum as (c+1))
        aggregate key(a,b,c)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");"""
        exception "The generated columns can be key columns, or value columns of replace and replace_if_not_null aggregation type."
    }
}