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

suite("test_pull_up_predicate_literal") {
    sql """ DROP TABLE IF EXISTS test_pull_up_predicate_literal; """
    sql "set enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute'"""
    sql 'set runtime_filter_mode=off'
    sql 'set enable_fold_constant_by_be=true'
    sql 'set debug_skip_fold_constant=false'
    sql 'set disable_join_reorder=true'

    sql """
     CREATE TABLE `test_pull_up_predicate_literal` (
    `col1` varchar(50),
    `col2` varchar(50)
    ) 
    PROPERTIES
    (
    "replication_num"="1"
    );
    """
    sql "insert into test_pull_up_predicate_literal values('abc','def'),(null,'def'),('abc',null)"
    sql """
     DROP view if exists test_pull_up_predicate_literal_view;
    """

    sql """
     create view test_pull_up_predicate_literal_view
    (
    `col1` ,
        `col2`
    )
    AS
    select
    tmp.col1,tmp.col2
    from (
    select 'abc' as col1,'def' as col2
    ) tmp
    inner join test_pull_up_predicate_literal ds on tmp.col1 = ds.col1  and tmp.col2 = ds.col2;
    """
    qt_test_pull_up_literal1 """
    explain shape plan select * from test_pull_up_predicate_literal_view;
    """

    qt_test_pull_up_literal2 """explain shape plan select * from test_pull_up_predicate_literal_view where col1='abc' and col2='def';"""
    qt_test_pull_up_literal_suquery """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2
                        FROM (
                                SELECT
                                sub.col1,
                                sub.col2
                                FROM (
                                        SELECT
                                        'abc' AS col1,
                                        'def' AS col2
                                                FROM
                                                test_pull_up_predicate_literal
                                ) sub
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """
    qt_test_pull_up_literal_extra_literal """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2,
                        tmp.col3
                        FROM (
                                SELECT
                                'abc' AS col1,
                                'def' AS col2,
                                'extra' AS col3
                                        FROM
                                        test_pull_up_predicate_literal
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """

    qt_test_pull_up_literal_with_agg_func """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2,
                        tmp.count_col
                        FROM (
                                SELECT
                                'abc' AS col1,
                                'def' AS col2,
                                COUNT(*) AS count_col
                                        FROM
                                        test_pull_up_predicate_literal
                                        GROUP BY
                                        col1, col2
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """

    qt_test_pull_up_literal_to_empty_relation """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2
                        FROM (
                                SELECT
                                'mno' AS col1,
                                'pqr' AS col2
                                        FROM
                                        test_pull_up_predicate_literal
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """
    qt_test_pull_up_literal_with_common_column """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2
                        FROM (
                                SELECT
                                'abc' AS col1,
                                col2
                                FROM
                                test_pull_up_predicate_literal
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """

    qt_test_pull_up_literal_outer_has_agg """
        explain shape plan
        SELECT MAX(t.col1), MIN(t.col2)
        FROM (
                SELECT tmp.col1, tmp.col2
                        FROM (
                        SELECT 'abc' AS col1, 'def' AS col2
                                FROM test_pull_up_predicate_literal
                ) tmp
                        INNER JOIN test_pull_up_predicate_literal ds
                        ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
        ) t
        WHERE col1 = 'abc' AND col2 = 'def';
    """


    qt_test_pull_up_literal_multi_join """
        explain shape plan
        SELECT *
                FROM (
                        SELECT tmp.col1, tmp.col2
                                FROM (
                                SELECT 'abc' AS col1, 'def' AS col2
                                        FROM test_pull_up_predicate_literal
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                        INNER JOIN test_pull_up_predicate_literal tmp2
                                ON tmp.col1 = tmp2.col1
                ) t
    """

    qt_test_pull_up_literal_outer_or """
        explain shape plan
        SELECT *
                FROM (
                        SELECT
                        tmp.col1,
                        tmp.col2
                        FROM (
                                SELECT
                                'abc' AS col1,
                                'def' AS col2
                                        FROM
                                        test_pull_up_predicate_literal
                        ) tmp
                                INNER JOIN test_pull_up_predicate_literal ds
                                ON tmp.col1 = ds.col1 AND tmp.col2 = ds.col2
                ) t
        WHERE (col1 = 'abc' AND col2 = 'def') OR (col1 = 'ghi' AND col2 = 'jkl');
    """




    sql "drop table if exists test_types"
    sql """
    CREATE TABLE test_types( d_tinyint tinyint, d_smallint SMALLINT,d_int int, d_bigint bigint, d_largeint largeint, d_bool boolean,
    d_float float, d_double double, d_decimal decimal(38,19),d_date date, d_datetime datetime,d_char char(255), d_varchar varchar(65533),
    d_string string, d_ipv4 ipv4, d_ipv6 ipv6, d_array ARRAY<int>) distributed by hash(d_int) properties("replication_num"="1");
    """
    sql """
    INSERT INTO test_types values(127,32767,32768,214748364799,922337203685477580722,TRUE,1.0001,1.000000000003,12232.2398272342335234,
    '2024-07-01','2024-08-02 10:10:00.123332', 'abcdsadtestchar','dtestvarcharvarchar','longlonglongstring','127.0.0.1','ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
    [12,3,5,2]);
    """
    sql """
    INSERT INTO test_types values(127,32767,32768,214748364799,922337203685477580722,TRUE,1.0001,1.000000000003,12232.2398272342335234,
    '2024-07-01','2024-08-22 10:10:00.123332', 'aaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbb','longlonglongstring','255.255.255.0','ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
    [12,0,0,888,22,1])
    """

    qt_const_value_and_join_column_type0 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type1 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type2 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type3 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type4 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type5 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type6 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type7 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type8 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type9 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type10 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type11 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type12 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type13 """
explain shape plan
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type16 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type17 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type18 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type19 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type20 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type21 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type22 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type23 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type24 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type25 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type26 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type27 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type28 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type29 """
explain shape plan
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type32 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type33 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type34 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type35 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type36 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type37 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type38 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type39 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type40 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type41 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type42 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type43 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type44 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type45 """
explain shape plan
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type48 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type49 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type50 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type51 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type52 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type53 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type54 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type55 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type56 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type57 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type58 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type59 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type60 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type61 """
explain shape plan
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type64 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type65 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type66 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type67 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type68 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type69 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type70 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type71 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type72 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type73 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type74 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type75 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type76 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type77 """
explain shape plan
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type80 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type81 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type82 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type83 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type84 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type85 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type86 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type87 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type88 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type89 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type90 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type91 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type92 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type93 """
explain shape plan
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type96 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type97 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type98 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type99 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type100 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type101 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type102 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type103 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type104 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type105 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type106 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type107 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type108 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type109 """
explain shape plan
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type112 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type113 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type114 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type115 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type116 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type117 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type118 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type119 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type120 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type121 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type122 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type123 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type124 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type125 """
explain shape plan
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type128 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type129 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type130 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type131 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type132 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type133 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type134 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type135 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type136 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type137 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type138 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type139 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type140 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type141 """
explain shape plan
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type144 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type145 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type146 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type147 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type148 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type149 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type150 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type151 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type152 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type153 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type154 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type155 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type156 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type157 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type158 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type159 """
explain shape plan
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type160 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type161 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type162 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type163 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type164 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type165 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type166 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type167 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type168 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type169 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1"""

    qt_const_value_and_join_column_type170 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1"""

    qt_const_value_and_join_column_type171 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type172 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type173 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type174 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type175 """
explain shape plan
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type176 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type177 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type178 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type179 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type180 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type181 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type182 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type183 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type184 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type187 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type188 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type189 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type190 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type191 """
explain shape plan
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type192 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type193 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type194 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type195 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type196 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type197 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type198 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type199 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type200 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type203 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type204 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type205 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type206 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type207 """
explain shape plan
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type208 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type209 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type210 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type211 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type212 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type213 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type214 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type215 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type216 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type219 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type220 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type221 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type222 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type223 """
explain shape plan
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type224 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type225 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type226 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type227 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type228 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type229 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type230 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type231 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type232 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type235 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type236 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type237 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type238 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type239 """
explain shape plan
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type240 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1"""

    qt_const_value_and_join_column_type241 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1"""

    qt_const_value_and_join_column_type242 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1"""

    qt_const_value_and_join_column_type243 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1"""

    qt_const_value_and_join_column_type244 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1"""

    qt_const_value_and_join_column_type245 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1"""

    qt_const_value_and_join_column_type246 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1"""

    qt_const_value_and_join_column_type247 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1"""

    qt_const_value_and_join_column_type248 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1"""

    qt_const_value_and_join_column_type251 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1"""

    qt_const_value_and_join_column_type252 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1"""

    qt_const_value_and_join_column_type253 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1"""

    qt_const_value_and_join_column_type254 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv4=t.c1"""

    qt_const_value_and_join_column_type255 """
explain shape plan
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1"""

    qt_const_value_and_join_column_type_res0 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res1 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res2 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res3 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res4 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res5 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res6 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res7 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res8 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res9 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res10 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res11 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res12 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res13 """
select c1 from (select 
127 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res16 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res17 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res18 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res19 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res20 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res21 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res22 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res23 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res24 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res25 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res26 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res27 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res28 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res29 """
select c1 from (select 
32767 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res32 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res33 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res34 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res35 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res36 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res37 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res38 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res39 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res40 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res41 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res42 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res43 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res44 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res45 """
select c1 from (select 
32768 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res48 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res49 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res50 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res51 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res52 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res53 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res54 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res55 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res56 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res57 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res58 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res59 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res60 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res61 """
select c1 from (select 
214748364799 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res64 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res65 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res66 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res67 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res68 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res69 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res70 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res71 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""


    qt_const_value_and_join_column_type_res73 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res74 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res75 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res76 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res77 """
select c1 from (select 
922337203685477580722 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res80 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res81 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res82 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res83 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res84 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res85 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res86 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res87 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res88 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res89 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res90 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res91 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res92 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res93 """
select c1 from (select 
TRUE as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res96 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res97 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res98 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res99 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res100 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res101 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res102 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res103 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res104 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res105 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res106 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res107 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res108 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res109 """
select c1 from (select 
1.0001 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res112 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res113 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res114 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res115 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res116 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res117 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res118 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res119 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res120 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res121 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res122 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res123 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res124 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res125 """
select c1 from (select 
1.000000000003 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res128 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res129 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res130 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res131 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res132 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res133 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res134 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res135 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res136 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res137 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res138 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res139 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res140 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res141 """
select c1 from (select 
12232.2398272342335234 as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res144 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res145 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res146 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res147 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res148 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res149 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res150 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res151 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res152 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res153 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res154 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res155 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res156 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res157 """
select c1 from (select 
'2024-07-01' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res160 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res161 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res162 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res163 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res164 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res165 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res166 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res167 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res168 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res169 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_date=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res170 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_datetime=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res171 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res172 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res173 """
select c1 from (select 
'2024-08-02 10:10:00.123332' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res176 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res177 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res178 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res179 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res180 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res181 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res182 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res183 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res184 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res188 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res189 """
select c1 from (select 
'abcdsadtestchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res192 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res193 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res194 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res195 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res196 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res197 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res198 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res199 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res200 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res203 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res204 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res205 """
select c1 from (select 
'dtestvarcharvarchar' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res208 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res209 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res210 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res211 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res212 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res213 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res214 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res215 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res216 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res219 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res220 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res221 """
select c1 from (select 
'longlonglongstring' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res224 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res225 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res226 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res227 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res228 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res229 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res230 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res231 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res232 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res235 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res236 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res237 """
select c1 from (select 
'127.0.0.1' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res240 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_tinyint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res241 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_smallint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res242 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_int=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res243 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bigint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res244 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_largeint=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res245 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_bool=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res246 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_float=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res247 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_double=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res248 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_decimal=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res251 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_char=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res252 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_varchar=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res253 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_string=t.c1 order by 1"""

    qt_const_value_and_join_column_type_res255 """
select c1 from (select 
'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' as c1 from test_pull_up_predicate_literal limit 10) t inner join test_types t2 on d_ipv6=t.c1 order by 1"""


    sql """ DROP TABLE IF EXISTS test_pull_up_predicate_literal; """
    sql """ DROP view if exists test_pull_up_predicate_literal_view; """
}

