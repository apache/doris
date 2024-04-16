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

suite("test_null_equal") {
    qt_test_const1 "select  null <=> null;"
    qt_test_const2 "select  null <=> 0;"
    qt_test_const3 "select  1 <=> null;"

    sql "drop table if exists test_eq_for_null_not_nullable;"
    sql """
    create table test_eq_for_null_not_nullable(
     k1 int not null
     ) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
    insert into test_eq_for_null_not_nullable values 
        (1),(2),(3);
    """
    sql "sync"
    qt_test1 "select * from test_eq_for_null_not_nullable where k1 <=> null;"
    qt_test2 "select * from test_eq_for_null_not_nullable where null <=> k1;"

    sql "drop table if exists test_eq_for_null_nullable;"
    sql """
    create table test_eq_for_null_nullable(
     k1 int 
     ) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
    insert into test_eq_for_null_nullable values 
        (1),(2),(3),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null),
        (null), (null), (null),(null),(null),(null),(null),(null); 
    """
    sql "sync"
    qt_test3 "select * from test_eq_for_null_not_nullable l, test_eq_for_null_nullable r where l.k1 <=> r.k1 order by 1;"
    qt_test4 "select * from test_eq_for_null_nullable where k1 <=> null;"
    qt_test5 "select * from test_eq_for_null_nullable where null <=> k1;"

    sql "drop table if exists test_eq_for_null_nullable2;"
    sql """
    create table test_eq_for_null_nullable2(
     k1 int 
     ) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
    insert into test_eq_for_null_nullable2 values 
        (null),(0),(1),(2),(3);
    """
    sql "sync"

    qt_test6 "select * from test_eq_for_null_nullable a, test_eq_for_null_nullable2 b where a.k1 <=> b.k1 order by 1;"

    qt_test7 "select * from test_eq_for_null_nullable2 where k1 <=> 1 order by 1;"


    sql """
        drop table if exists test_eq_for_null_non_const_table1;
    """
    sql """
        drop table if exists test_eq_for_null_non_const_table2;
    """

    sql """
        create table test_eq_for_null_non_const_table1 (
            col_int_undef_signed int,
            col_varchar_10__undef_signed varchar(10) ,
            pk int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test_eq_for_null_non_const_table1(pk,col_int_undef_signed,col_varchar_10__undef_signed) values
            (0,4,'will'),(1,null,''),(2,null,'y'),(3,null,''),(4,3,'the'),(5,6,'when'),(6,8,''),(7,6,''),(8,null,null);
    """

    sql """
        create table test_eq_for_null_non_const_table2 (
        col_int_undef_signed int,
        col_varchar_10__undef_signed varchar(10),
        pk int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test_eq_for_null_non_const_table2(pk,col_int_undef_signed,col_varchar_10__undef_signed) values
            (0,null,null),(1,null,'tell'),(2,null,null),(3,null,'is'),(4,null,'oh'),(5,1,'who'),(6,7,''),
            (7,1,'I''m'),(8,null,null),(9,0,'how'),(10,null,null),(11,4,'why'),(12,null,'o'),(13,null,'he''s'),
            (14,6,''),(15,0,''),(16,7,''),(17,7,'good'),(18,8,''),(19,null,''),(20,6,'');
    """

    qt_test8 """
         SELECT *
         FROM test_eq_for_null_non_const_table1 AS t1
         WHERE t1.`pk` NOT IN (
            SELECT
                `pk`
            FROM test_eq_for_null_non_const_table2 AS t2
            WHERE
                ( t1. col_int_undef_signed  >  t2 . col_int_undef_signed + 1 )
                OR  t1. col_int_undef_signed + 3  <=>  t2 . col_int_undef_signed
         )
         ORDER BY 1, 2, 3;
    """
}