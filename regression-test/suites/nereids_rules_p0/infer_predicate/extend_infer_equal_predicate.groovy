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
suite("extend_infer_equal_predicate") {
    sql "set enable_fallback_to_original_planner=false"
    sql """SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"""
    sql 'set runtime_filter_mode=off'
    sql 'set disable_join_reorder=true'

    sql """
    drop table if exists test_cast_infer9;
    """
    sql """
    drop table if exists test_cast_infer8;
    """
    sql """
    create table test_cast_infer9(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2,d_date date, d_datetime datetime) properties('replication_num'='1');
    """
    sql """
    create table test_cast_infer8(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2,d_date date, d_datetime datetime) properties('replication_num'='1');
    """
    sql """
    insert into test_cast_infer9 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02');
    """
    sql """
    insert into test_cast_infer8 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02');
    """

    sql "drop table if exists test_like1;"
    sql "drop table if exists test_like2;"
    sql "drop table if exists test_like3;"

    sql """
    CREATE TABLE `test_like1` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE `test_like2` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE `test_like3` (
    `a` INT NULL,
    `b` VARCHAR(10) NULL,
    `c` INT NULL,
    `d` INT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    DISTRIBUTED BY RANDOM BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    insert into test_like1 values(1,'d2',3,5);
    """
    sql """
    insert into test_like2 values(1,'d2',2,2);
    """
    sql """
    insert into test_like3 values(1,'d2',2,2);
    """
    sql """
    insert into test_like2 values(-3,'d2',2,2);
    """
    sql """
    insert into test_like1 values(0,'d2',3,5);
    """

    qt_test_integer_cast """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_tinyint where t1.d_tinyint<10;"""
    qt_test_simple_compare """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int where t2.d_int<10"""
    qt_test_simple_compare_not_equal """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int where t2.d_int!=10;"""
    qt_test_simple_compare_datetimev2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2='2024-01-01';"""
    qt_test_simple_compare_not_equal_datetimev2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2!='2024-01-01';"""
    qt_test_not_in """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int where t2.d_int not in (10,20)"""
    qt_test_in """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int where t2.d_int in (10,20)"""
    qt_test_func_not_in """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int where abs(t2.d_int) not in (10,20)"""
    qt_test_like """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012%'"""
    qt_test_like_not """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 not like '012%'"""
    qt_test_like_to_equal """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012'"""
    qt_test_func_not_in_and_func_equal_condition """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on abs(t1.d_int)=abs(t2.d_int) where abs(t2.d_int) not in (10,20)"""

    qt_test_between_and """explain shape plan
    select * from test_like1 t1 ,test_like2 t2 where t1.a=t2.a and t1.a between 1 and 10;"""
    qt_test_and """explain shape plan
    select * from test_like1 t1 ,test_like2 t2 where t1.a=t2.a and  (t1.a >=2 and t1.a<=10);"""
    qt_test_or1 """explain shape plan
    select * from test_like1 t1 ,test_like2 t2 where t1.a=t2.a and not t1.a between 2 and 10;"""
    qt_test_or2 """explain shape plan
    select * from test_like1 t1 ,test_like2 t2 where t1.a=t2.a and not (t1.a >=2 and t1.a<=10);"""
    qt_test_sign_predicate """explain shape plan
    select * from test_like1 t1 ,test_like2 t2 where t1.a=t2.a and sign(t1.a)>=1"""
    qt_test_if_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int
    where case when t2.d_int not in (10,20) then true else false end"""
    qt_test_if_and_in_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) not in (FALSE)"""
    qt_test_if_and_in_predicate_not """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) !=FALSE"""
    qt_test_multi_slot_in_predicate1 """explain shape plan
    select * from test_like1 t1 inner join test_like2 t2 on t1.a+t1.c=t2.a+t2.c and t1.a+t1.c<10"""
    qt_test_multi_slot_in_predicate2 """explain shape plan
    select * from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a and t1.b=t2.b and t1.a+t1.b<10"""
    qt_test_case_when_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_int
    where case when t2.d_int=1 then true when  t2.d_int=2 then false else false end"""
    qt_test_datetimev2_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_datetimev2=t2.d_datetimev2 where convert_tz(date_trunc(t2.d_datetimev2, 'month'),'Asia/Shanghai','Europe/Paris')='2024-01-01';"""

    // function predicate
    qt_test_convert_tz_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris')>'2022-01-01';"""
    qt_test_next_date_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10;"""
    qt_test_random_nest_predicate """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),random(1,10)))>10;"""
    qt_test_random_predicate """explain shape plan
    select * from test_like1 t1 inner join test_like2 t2 on t1.a=t2.a and t1.a>random(10);"""
    qt_test_predicate_map """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10
    and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') < '2022-01-01';"""

    // test cast
    qt_test_int_upcast """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int=t2.d_tinyint where t2.d_tinyint<10;"""
    qt_test_int_downcast """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(t1.d_int as tinyint)=t2.d_tinyint where t2.d_tinyint<10;"""
    qt_test_date_upcast """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2 on t1.d_datev2 =t2.d_datetimev2 and t1.d_datev2<'2022-01-03';"""
    qt_test_date_downcast """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2 on t1.d_datev2 =cast(t2.d_datetimev2 as datev2) and t1.d_datev2<'2022-01-03';"""
    qt_test_date_both_upcast1 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2 on cast(t1.d_datev2 as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datev2<'2022-01-03';"""
    qt_test_date_both_upcast2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2 on cast(t1.d_datetime as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datetime<'2022-01-03';"""
    // cast char behave differently because of substring
    qt_test_char_different_type1 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_char100=t2.d_char10 and t2.d_char10>'abc';"""
    qt_test_char_different_type2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(t1.d_char100 as char(50))=t2.d_char10 and t2.d_char10>'abc';"""
    qt_test_char_different_type3 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(t1.d_char100 as char(50))=cast(t2.d_char10 as char(50)) and t2.d_char10>'abc';"""
    qt_test_char_different_type4 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(t1.d_char100 as char(200))=cast(t2.d_char10 as char(200)) and t2.d_char10>'abc';"""

    qt_test_cast_and_func """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on abs(t1.d_int)=t2.d_tinyint where t2.d_tinyint<10 ;"""
    qt_test_cast_and_func2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(abs(t1.d_int) as tinyint)=t2.d_tinyint where t2.d_tinyint<10;"""
    qt_test_cast_and_func3 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on cast(t1.d_int as tinyint)=abs(t2.d_tinyint) where abs(t2.d_tinyint)<10;"""
    qt_test_cast_and_func4 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer9 t2 on t1.d_int =abs(t2.d_tinyint) where abs(t2.d_tinyint)<10;"""
    qt_test_func_equal_and_nest_func_pred1 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10;"""
    qt_test_func_equal_and_nest_func_pred2 """explain shape plan
    select * from test_cast_infer9 t1 inner join test_cast_infer8 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'))>10;"""
    qt_predicate_to_empty_relation """explain shape plan
    select * from test_like1 t1 left join test_like2 t2 on t1.a=t2.a and t2.a=1 left join test_like2 t3 on t1.a=t3.a where t1.a=2"""
    qt_equal_table_predicate_delete """
    explain shape plan select * from test_like1 where a=1 and c=1;
    """
}