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

suite("test_generated_column_delete") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    multi_sql """
        drop table if exists test_par_gen_col;
        create table test_par_gen_col (a int, b int, c int as (a+b))
        partition by range(c) 
        (
        partition p1 values [('1'),('10')),
        partition p2 values [('10'),('20')),
        partition p3 values [('20'),('30'))
        )
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES ("replication_num" = "1");
        insert into test_par_gen_col values(1,2,default),(10,2,default),(2,22,default);
        
        drop table if exists test_par_gen_col_unique;
        create table test_par_gen_col_unique (a int, b int, c int as (a+b)) unique key(a,b,c)
        partition by range(c) 
        (
        partition p1 values [('1'),('10')),
        partition p2 values [('10'),('20')),
        partition p3 values [('20'),('30'))
        )
        DISTRIBUTED BY hash(c)
        PROPERTIES ("replication_num" = "1");
        insert into test_par_gen_col_unique values(1,2,default),(10,2,default),(2,22,default),(10,2,default);
    """

    sql "delete from test_par_gen_col partition p1 where c=3;"
    qt_delete_where_gen_col_select "select * from test_par_gen_col order by a,b,c;"
    sql "delete from test_par_gen_col partition p1 where c=12;"
    qt_delete_where_gen_col_partition_has_no_satisfied_row_select "select * from test_par_gen_col order by a,b,c;;"
    sql "delete from test_par_gen_col partition p2 where c=12 and a=10;"
    qt_delete_where_gen_col_and_other_col_select "select * from test_par_gen_col order by a,b,c;;"

    sql "delete from test_par_gen_col_unique partition p1 where c=3;"
    qt_delete_where_gen_col_select_unique "select * from test_par_gen_col_unique order by a,b,c;;"
    sql "delete from test_par_gen_col_unique partition p1 where c=12;"
    qt_delete_where_gen_col_partition_has_no_satisfied_row_select_unique "select * from test_par_gen_col_unique order by a,b,c;;"
    sql "delete from test_par_gen_col_unique partition p2 where c=12 and a=10;"
    qt_delete_where_gen_col_and_other_col_select_unique "select * from test_par_gen_col_unique order by a,b,c;"

    sql """delete from test_par_gen_col_unique t1 using test_par_gen_col t2 inner join test_par_gen_col t3
     on t2.b=t3.b where t1.c=t2.c and t1.b=t2.b"""
    qt_delete_query_select "select * from test_par_gen_col_unique order by a,b,c;"
    sql "insert into test_par_gen_col_unique values(1,2,default),(10,2,default),(2,22,default),(10,2,default);"
    sql """
    with cte as(
        select t2.* from
        test_par_gen_col t2 inner join test_par_gen_col t3 on t2.b=t3.b
    ) delete from test_par_gen_col_unique t1 using cte where t1.c=cte.c and t1.b=cte.b"""
    qt_delete_query_cte_select "select * from test_par_gen_col_unique order by a,b,c"

    multi_sql """
        drop table if exists test_gen_col_mow_delete_partial_update;
        create table test_gen_col_mow_delete_partial_update (
            a int,
            b int,
            c int as (b + 1),
            d int
        )
        unique key(a)
        distributed by hash(a) buckets 1
        properties (
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "false",
            "replication_num" = "1"
        );
        insert into test_gen_col_mow_delete_partial_update(a, b, d) values (1, 10, 100), (2, 20, 200);
    """
    sql "delete from test_gen_col_mow_delete_partial_update where a = 1;"
    qt_delete_mow_without_light_delete_generated_column """
        select * from test_gen_col_mow_delete_partial_update order by a;
    """

    multi_sql """
        drop table if exists test_gen_key_mow_delete_partial_update;
        create table test_gen_key_mow_delete_partial_update (
            a int,
            c int as (b + 1),
            b int,
            d int
        )
        unique key(a, c)
        distributed by hash(a) buckets 1
        properties (
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "false",
            "replication_num" = "1"
        );
        insert into test_gen_key_mow_delete_partial_update(a, b, d) values (1, 10, 100), (2, 20, 200);
    """
    sql "delete from test_gen_key_mow_delete_partial_update where a = 1;"
    qt_delete_mow_without_light_delete_generated_key_column """
        select * from test_gen_key_mow_delete_partial_update order by a;
    """

    multi_sql """
        drop table if exists test_gen_col_mow_delete_partial_update_not_null_value;
        create table test_gen_col_mow_delete_partial_update_not_null_value (
            a int,
            b int,
            c int as (b + 1),
            d int not null
        )
        unique key(a)
        distributed by hash(a) buckets 1
        properties (
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "false",
            "replication_num" = "1"
        );
        insert into test_gen_col_mow_delete_partial_update_not_null_value(a, b, d)
            values (1, 10, 100), (2, 20, 200);
    """
    sql "delete from test_gen_col_mow_delete_partial_update_not_null_value where a = 1;"
    qt_delete_mow_without_light_delete_generated_column_with_not_null_value """
        select * from test_gen_col_mow_delete_partial_update_not_null_value order by a;
    """

    multi_sql """
        drop table if exists test_gen_variant_mow_delete_partial_update;
        create table test_gen_variant_mow_delete_partial_update (
            id int not null,
            create_time datetime not null,
            order_no varchar(128) not null,
            receive_address_detail varchar(1024) not null default "{}",
            d int not null,
            new_col variant as (receive_address_detail) null
        )
        unique key(id, create_time, order_no)
        distributed by hash(order_no) buckets 1
        properties (
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "false",
            "replication_num" = "1"
        );
        insert into test_gen_variant_mow_delete_partial_update(
            id, create_time, order_no, receive_address_detail, d
        ) values
            (1, '2026-06-08 10:00:00', 'order-1', '{"city":"sh"}', 100),
            (2, '2026-06-08 11:00:00', 'order-2', '{"city":"bj"}', 200);
    """
    sql "delete from test_gen_variant_mow_delete_partial_update where id = 1;"
    qt_delete_mow_without_light_delete_variant_generated_column """
        select id, create_time, order_no, receive_address_detail, d
        from test_gen_variant_mow_delete_partial_update order by id;
    """


}
