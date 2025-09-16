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
    drop table if exists extend_infer_t1;
    """
    sql """
    drop table if exists extend_infer_t2;
    """
    sql """
    create table extend_infer_t1(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2,d_date date, d_datetime datetime) properties('replication_num'='1');
    """
    sql """
    create table extend_infer_t2(d_int int, d_char100 char(100), d_smallint smallint, d_tinyint tinyint, d_char10 char(10),d_datetimev2 datetimev2, d_datev2 datev2,d_date date, d_datetime datetime) properties('replication_num'='1');
    """
    sql """
    insert into extend_infer_t1 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02');
    """
    sql """
    insert into extend_infer_t2 values(1,'01234567890123456789', 3,3,'0123456789','2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00'),(14,'01234567890123456789', 33,23,'0123456789','2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02');
    """

    sql "drop table if exists extend_infer_t3;"
    sql "drop table if exists extend_infer_t4;"
    sql "drop table if exists extend_infer_t5;"

    sql """
    CREATE TABLE `extend_infer_t3` (
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
    CREATE TABLE `extend_infer_t4` (
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
    CREATE TABLE `extend_infer_t5` (
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
    insert into extend_infer_t3 values(100,'d2',3,5),(0,'d2',3,5),(null,null,9,3),(33,'d2',2,5),(null,'d2',3,55),(78,null,9,3),(12,null,9,3);
    """
    sql """
    insert into extend_infer_t4 values(10,'d2',2,2),(0,'d2',2,2),(100,'d2',3,null),(null,null,9,3),(78,'d2',23,5),(33,'d2',23,5);
    """
    sql """
    insert into extend_infer_t5 values(1,'d2',2,2);
    """

    qt_test_integer_cast """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint where t1.d_tinyint<10;"""
    qt_test_simple_compare """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int<10"""
    qt_test_simple_compare_not_equal """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int!=10;"""
    qt_test_simple_compare_datetimev2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2='2024-01-01';"""
    qt_test_simple_compare_not_equal_datetimev2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2!='2024-01-01';"""
    qt_test_not_in """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int not in (10,20)"""
    qt_test_in """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int in (10,20)"""
    qt_test_func_not_in """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where abs(t2.d_int) not in (10,20)"""
    qt_test_like """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012%'"""
    qt_test_like_not """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 not like '012%'"""
    qt_test_like_to_equal """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012'"""
    qt_test_func_not_in_and_func_equal_condition """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on abs(t1.d_int)=abs(t2.d_int) where abs(t2.d_int) not in (10,20)"""

    qt_test_between_and """explain shape plan
    select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and t1.a between 1 and 10;"""
    qt_test_and """explain shape plan
    select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and  (t1.a >=2 and t1.a<=10);"""
    qt_test_or1 """explain shape plan
    select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and not t1.a between 2 and 10;"""
    qt_test_or2 """explain shape plan
    select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and not (t1.a >=2 and t1.a<=10);"""
    qt_test_sign_predicate """explain shape plan
    select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and sign(t1.a)>=1"""
    qt_test_if_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where case when t2.d_int not in (10,20) then true else false end"""
    qt_test_if_and_in_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) not in (FALSE)"""
    qt_test_if_and_in_predicate_not """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) !=FALSE"""
    qt_test_multi_slot_in_predicate1 """explain shape plan
    select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a+t1.c=t2.a+t2.c and t1.a+t1.c<10"""
    qt_test_multi_slot_in_predicate2 """explain shape plan
    select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a=t2.a and t1.b=t2.b and t1.a+t1.b<10"""
    qt_test_case_when_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where case when t2.d_int=1 then true when  t2.d_int=2 then false else false end"""
    qt_test_datetimev2_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where convert_tz(date_trunc(t2.d_datetimev2, 'month'),'Asia/Shanghai','Europe/Paris')='2024-01-01';"""

    // function predicate
    qt_test_convert_tz_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris')>'2022-01-01';"""
    qt_test_next_date_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10;"""
    qt_test_random_nest_predicate """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),random(1,10)))>10;"""
    qt_test_random_predicate """explain shape plan
    select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a=t2.a and t1.a>random(10);"""
    qt_test_predicate_map """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10
    and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') < '2022-01-01';"""

    // test cast
    qt_test_int_upcast """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint where t2.d_tinyint<10;"""
    qt_test_int_downcast """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_int as tinyint)=t2.d_tinyint where t2.d_tinyint<10;"""
    qt_test_date_upcast """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_datev2 =t2.d_datetimev2 and t1.d_datev2<'2022-01-03';"""
    qt_test_date_downcast """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_datev2 =cast(t2.d_datetimev2 as datev2) and t1.d_datev2<'2022-01-03';"""
    qt_test_date_both_upcast1 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on cast(t1.d_datev2 as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datev2<'2022-01-03';"""
    qt_test_date_both_upcast2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on cast(t1.d_datetime as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datetime<'2022-01-03';"""
    // cast char behave differently because of substring
    qt_test_char_different_type1 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char10 and t2.d_char10>'abc';"""
    qt_test_char_different_type2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(50))=t2.d_char10 and t2.d_char10>'abc';"""
    qt_test_char_different_type3 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(50))=cast(t2.d_char10 as char(50)) and t2.d_char10>'abc';"""
    qt_test_char_different_type4 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(200))=cast(t2.d_char10 as char(200)) and t2.d_char10>'abc';"""

    qt_test_cast_and_func """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on abs(t1.d_int)=t2.d_tinyint where t2.d_tinyint<10 ;"""
    qt_test_cast_and_func2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(abs(t1.d_int) as tinyint)=t2.d_tinyint where t2.d_tinyint<10;"""
    // this should be inferred but not
    qt_test_cast_and_func3 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_int as tinyint)=abs(t2.d_tinyint) where abs(t2.d_tinyint)<10;"""
    qt_test_cast_and_func4 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int =abs(t2.d_tinyint) where abs(t2.d_tinyint)<10;"""
    qt_test_func_equal_and_nest_func_pred1 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10;"""
    qt_test_func_equal_and_nest_func_pred2 """explain shape plan
    select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'))>10;"""
    qt_predicate_to_empty_relation """explain shape plan
    select * from extend_infer_t3 t1 left join extend_infer_t4 t2 on t1.a=t2.a and t2.a=1 left join extend_infer_t4 t3 on t1.a=t3.a where t1.a=2"""
    qt_equal_table_predicate_delete """
    explain shape plan select * from extend_infer_t3 where a=1 and c=1;
    """

    qt_test_integer_cast_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint where t1.d_tinyint<10 order by t1.d_int;;"""
    qt_test_simple_compare_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int<10 order by t1.d_int;"""
    qt_test_simple_compare_not_equal_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int!=10 order by t1.d_int;"""
    qt_test_simple_compare_datetimev2_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2='2024-01-01' order by t1.d_int;;"""
    qt_test_simple_compare_not_equal_datetimev2_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where t2.d_datetimev2!='2024-01-01' order by t1.d_int;;"""
    qt_test_not_in_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int not in (10,20) order by t1.d_int;"""
    qt_test_in_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t2.d_int in (10,20) order by t1.d_int ;"""
    qt_test_func_not_in_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where abs(t2.d_int) not in (10,20) order by t1.d_int;"""
    qt_test_like_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012% order by t1.d_int;'"""
    qt_test_like_not_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 not like '012%' order by t1.d_int;"""
    qt_test_like_to_equal_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char100 where t2.d_char100 like '012' order by t1.d_int;"""
    qt_test_func_not_in_and_func_equal_condition_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on abs(t1.d_int)=abs(t2.d_int) where abs(t2.d_int) not in (10,20) order by t1.d_int;"""

    qt_test_between_and_res """select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and t1.a between 1 and 10 order by 1,2,3,4,5,6,7,8;"""
    qt_test_and_res """select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and  (t1.a >=2 and t1.a<=10)  order by 1,2,3,4,5,6,7,8;"""
    qt_test_or1_res """select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and not t1.a between 2 and 10  order by 1,2,3,4,5,6,7,8;"""
    qt_test_or2_res """select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and not (t1.a >=2 and t1.a<=10) order by 1,2,3,4,5,6,7,8;"""
    qt_test_sign_predicate_res """select * from extend_infer_t3 t1 ,extend_infer_t4 t2 where t1.a=t2.a and sign(t1.a)>=1 order by 1,2,3,4,5,6,7,8"""
    qt_test_if_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where case when t2.d_int not in (10,20) then true else false end  order by 1,2,3,4,5,6,7,8"""
    qt_test_if_and_in_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) not in (FALSE)  order by 1,2,3,4,5,6,7,8"""
    qt_test_if_and_in_predicate_not_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where  if(t2.d_int =5,true, false) !=FALSE order by 1,2,3,4,5,6,7,8"""
    qt_test_multi_slot_in_predicate1_res """select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a+t1.c=t2.a+t2.c and t1.a+t1.c<10  order by 1,2,3,4,5,6,7,8"""
    qt_test_multi_slot_in_predicate2_res """select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a=t2.a and t1.b=t2.b and t1.a+t1.b<10  order by 1,2,3,4,5,6,7,8"""
    qt_test_case_when_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int
    where case when t2.d_int=1 then true when  t2.d_int=2 then false else false end  order by t1.d_int"""
    qt_test_datetimev2_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_datetimev2=t2.d_datetimev2 where convert_tz(date_trunc(t2.d_datetimev2, 'month'),'Asia/Shanghai','Europe/Paris')='2024-01-01'  order by t1.d_int;"""

    // function predicate
    qt_test_convert_tz_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris')>'2022-01-01'  order by t1.d_int;"""
    qt_test_next_date_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10  order by t1.d_int;"""
    qt_test_random_nest_predicate_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),random(1,10)))>10  order by t1.d_int;"""
    qt_test_random_predicate_res """select * from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a=t2.a and t1.a>random(10) order by 1,2,3,4,5,6,7,8;"""
    qt_test_predicate_map_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on t1.d_datetimev2 =t2.d_datetimev2 and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10
    and convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') < '2022-01-01' order by t1.d_int;"""

    // test cast
    qt_test_int_upcast_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint where t2.d_tinyint<10  order by t1.d_int;"""
    qt_test_int_downcast_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_int as tinyint)=t2.d_tinyint where t2.d_tinyint<10 order by t1.d_int;"""
    qt_test_date_upcast_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_datev2 =t2.d_datetimev2 and t1.d_datev2<'2022-01-03' order by t1.d_int;"""
    qt_test_date_downcast_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_datev2 =cast(t2.d_datetimev2 as datev2) and t1.d_datev2<'2022-01-03' order by t1.d_int;"""
    qt_test_date_both_upcast1_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on cast(t1.d_datev2 as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datev2<'2022-01-03'  order by t1.d_int;"""
    qt_test_date_both_upcast2_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2 on cast(t1.d_datetime as datetimev2)=cast(t2.d_date as datetimev2)
    and t1.d_datetime<'2022-01-03' order by t1.d_int;"""
    // cast char behave differently because of substring
    qt_test_char_different_type1_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_char100=t2.d_char10 and t2.d_char10>'abc' order by t1.d_int;"""
    qt_test_char_different_type2_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(50))=t2.d_char10 and t2.d_char10>'abc' order by t1.d_int;"""
    qt_test_char_different_type3_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(50))=cast(t2.d_char10 as char(50)) and t2.d_char10>'abc' order by t1.d_int;"""
    qt_test_char_different_type4_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_char100 as char(200))=cast(t2.d_char10 as char(200)) and t2.d_char10>'abc' order by t1.d_int;"""

    qt_test_cast_and_func_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on abs(t1.d_int)=t2.d_tinyint where t2.d_tinyint<10  order by t1.d_int;"""
    qt_test_cast_and_func2_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(abs(t1.d_int) as tinyint)=t2.d_tinyint where t2.d_tinyint<10 order by t1.d_int;"""
    qt_test_cast_and_func3_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on cast(t1.d_int as tinyint)=abs(t2.d_tinyint) where abs(t2.d_tinyint)<10 order by t1.d_int;"""
    qt_test_cast_and_func4_res """select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int =abs(t2.d_tinyint) where abs(t2.d_tinyint)<10 order by t1.d_int;"""
    qt_test_func_equal_and_nest_func_pred1_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(hours_add(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'),10))>10 order by t1.d_int;"""
    qt_test_func_equal_and_nest_func_pred2_res """select * from extend_infer_t1 t1 inner join extend_infer_t2 t2
    on convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris') =convert_tz(t2.d_datetimev2,'Asia/Shanghai','Europe/Paris') 
    and day(convert_tz(t1.d_datetimev2,'Asia/Shanghai','Europe/Paris'))>10 order by t1.d_int;"""
    qt_predicate_to_empty_relation_res """select * from extend_infer_t3 t1 left join extend_infer_t4 t2 on t1.a=t2.a and t2.a=1 left join extend_infer_t4 t3 on t1.a=t3.a where t1.a=2"""
    qt_equal_table_predicate_delete_res """select * from extend_infer_t3 where a=1 and c=1 order by 1,2,3,4;"""

    // non-inner join
    qt_not_equal_inner_left """explain shape plan
    select * from extend_infer_t1 t3 inner join (
	select t1.d_int as c1 from extend_infer_t1 t1 left join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1!=10;"""
    qt_not_equal_inner_left2 """explain shape plan
    select * from extend_infer_t1 t3 inner join (
	select t2.d_int as c1 from extend_infer_t1 t1 left join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1!=10;"""
    qt_not_equal_left_inner """explain shape plan
    select * from extend_infer_t1 t3 left join (
	select t1.d_int as c1 from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1!=10;"""
    qt_not_equal_left_left """explain shape plan
    select * from extend_infer_t1 t3 left join (
	select t1.d_int as c1 from extend_infer_t1 t1 left join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1!=10;"""
    qt_not_equal_left_left2 """explain shape plan
    select * from extend_infer_t1 t3 left join (
	select t2.d_int as c1 from extend_infer_t1 t1 left join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1!=10;"""

    qt_not_in_inner_right """explain shape plan
    select * from extend_infer_t1 t3 inner join (
	select t1.d_int as c1 from extend_infer_t1 t1 right join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1 not in (10,20);"""
    qt_not_in_inner_right2 """explain shape plan
    select * from extend_infer_t1 t3 inner join (
	select t2.d_int as c1 from extend_infer_t1 t1 right join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1 not in (10,20);"""
    qt_not_in_right_inner """explain shape plan
    select * from extend_infer_t1 t3 right join (
	select t1.d_int as c1 from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1 not in (10,20);"""
    qt_not_in_right_right """explain shape plan
    select * from extend_infer_t1 t3 right join (
	select t1.d_int as c1 from extend_infer_t1 t1 right join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1 not in (10,20);"""
    qt_not_in_right_right2 """explain shape plan
    select * from extend_infer_t1 t3 right join (
	select t2.d_int as c1 from extend_infer_t1 t1 right join extend_infer_t1 t2 on t1.d_int=t2.d_int) t on t3.d_int=t.c1 where t.c1 not in (10,20);"""

    qt_not_equal_semi_semi_with_cast """explain shape plan 
    select * from extend_infer_t1 t3 left semi join (
	select t1.d_int as c1 from extend_infer_t1 t1 left semi join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint) t 
	on t3.d_smallint=t.c1 where t3.d_smallint !=10;"""
    qt_not_equal_anti_anti_with_cast """explain shape plan 
    select * from extend_infer_t1 t3 left anti join (
	select t1.d_int as c1 from extend_infer_t1 t1 left anti join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint) t 
	on t3.d_smallint=t.c1 where t3.d_smallint !=10;"""
    qt_not_equal_anti_left_with_cast """explain shape plan 
    select * from extend_infer_t1 t3 left anti join (
	select t1.d_int as c1 from extend_infer_t1 t1 left join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint) t 
	on t3.d_smallint=t.c1 where t3.d_smallint !=10;"""
    qt_not_equal_semi_anti_with_cast """explain shape plan 
    select * from extend_infer_t1 t3 left semi join (
	select t1.d_int as c1 from extend_infer_t1 t1 left anti join extend_infer_t1 t2 on t1.d_int=t2.d_tinyint) t 
	on t3.d_smallint=t.c1 where t3.d_smallint !=10;"""
    qt_in_subquery_to_semi_join """explain shape plan
	select * from extend_infer_t1 t1 where t1.d_int in (select d_int from extend_infer_t2 where d_int != 10)
	"""
    // should not infer
    qt_not_in_subquery_to_na_anti_join_not_infer """explain shape plan
	select * from extend_infer_t1 t1 where t1.d_int not in (select d_int from extend_infer_t2 ) and t1.d_int !=10
	"""
    qt_in_subquery_to_semi_join """explain shape plan
	select * from extend_infer_t1 t1 inner join extend_infer_t1 t2 on t1.d_int=t2.d_int where t1.d_int in (select d_int from extend_infer_t2 where d_int != 10)
	"""

    qt_cast_to_decimal_overflow_not_infer """explain shape plan
    select 1 from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_tinyint=t2.d_int and t1.d_tinyint in(0.5,0.1)"""
    qt_char_equal_int_infer """explain shape plan
    select 1 from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_char10=t2.d_int and t1.d_char10 in('d','bb')"""
    qt_date_equal_int_infer """explain shape plan
    select 1 from extend_infer_t1 t1 inner join extend_infer_t2 t2 on t1.d_datev2=t2.d_int and t1.d_datev2 in('2024-01-01','2024-01-02')"""

    qt_not_pull_up_grouping """select a, b from extend_infer_t3 where a>0 group by grouping sets((a),(b)) having a>0 order by 1,2;"""
    qt_pull_up_grouping """select a, b from extend_infer_t3 where a>0 group by grouping sets((a),(a,b)) having a>0 order by 1,2;"""
    qt_pull_up_limit """select a from (select a, b from extend_infer_t3 where a<30 group by grouping sets((a),(a,b))  limit 10 ) t where a<30 order by 1;"""
    qt_pull_up_topn """select a from (select a, b from extend_infer_t3 where a<30 group by grouping sets((a),(a,b)) having a>1 order by a,b limit 10 ) t where a<30  order by 1"""

    qt_pull_up_window_partition_column """select c1,a,c from (select a,c,sum(a) over(partition by c order by a) c1 from extend_infer_t3 where c<33 ) t where a<33 and c<33  order by 1,2,3"""
    qt_pull_up_window_order_column """select c1,a from (select a,b,sum(a) over(order by a) c1 from extend_infer_t3 where a<33 ) t where a<33  order by 1,2"""
    qt_pull_up_partition_topn """select * from (select a, c,row_number() over(partition by b order by c) as rn from extend_infer_t3 where a>5 and c>3)t
    where a>5  and c>3  and rn<3 order by 1,2,3;"""
    qt_pull_up_generate """select a,b, age from (select * from extend_infer_t3 lateral view
    EXPLODE(ARRAY(30,60))  t1 as age where a<10 ) t group by grouping sets ((age),(a,b)) having a <10 order by 1,2,3"""

    qt_pull_up_from_inner_join """select a,b from (select t1.a,t2.b from extend_infer_t3 t1 inner join extend_infer_t4 t2 on t1.a=t2.a where t1.a<10  limit 10) t  where a<10 order by 1,2"""
    qt_pull_up_from_left_join """select a,b from (select t2.a,t2.b from extend_infer_t3 t1 left join extend_infer_t4 t2 on t1.a=t2.a and t2.a<10  limit 10) t  where a<10 order by 1,2"""
    qt_pull_up_from_left_semi_join "select a from (select t1.a from extend_infer_t3 t1 left semi join extend_infer_t4 t2 on t1.a=t2.a and t2.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_left_anti_join "select a from (select t1.a from extend_infer_t3 t1 left anti join extend_infer_t4 t2 on t1.a=t2.a and t1.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_left_null_aware_anti_join "select a from (select t1.a from extend_infer_t3 t1 where t1.a<100 and t1.a not in (select t2.a from extend_infer_t4 t2 where t2.a is not null) limit 10) t where a<100 order by 1"
    qt_pull_up_from_left_anti_join_where "select a from (select t1.a from extend_infer_t3 t1 left anti join extend_infer_t4 t2 on t1.a=t2.a where t1.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_right_join "select a,b from (select t1.a,t2.b from extend_infer_t3 t1 right join extend_infer_t4 t2 on t1.a=t2.a and t1.a<10  limit 10) t  where a<10 order by 1,2"
    qt_pull_up_from_right_semi_join "select a from (select t2.a from extend_infer_t3 t1 right semi join extend_infer_t4 t2 on t1.a=t2.a and t1.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_right_anti_join_where "select a from (select t2.a from extend_infer_t3 t1 right anti join extend_infer_t4 t2 on t1.a=t2.a where t2.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_right_anti_join_and "select a from (select t2.a from extend_infer_t3 t1 right anti join extend_infer_t4 t2 on t1.a=t2.a and t2.a<10  limit 10) t  where a<10 order by 1"
    qt_pull_up_from_union """select a from(select a from (select t1.a from extend_infer_t3 t1 where t1.a<10 union all select t2.a from extend_infer_t4 t2 where t2.a<10  ) tt
            limit 10) t  where a<10 order by 1;"""
    qt_pull_up_from_except """select a from(select a from (select t1.a from extend_infer_t3 t1 where t1.a<10 except select t2.a from extend_infer_t4 t2 where t2.a<10  ) tt
            limit 10) t  where a<10 order by 1;"""
    qt_pull_up_from_intersect """select a from(select a from (select t1.a from extend_infer_t3 t1 where t1.a<10 intersect select t2.a from extend_infer_t4 t2 where t2.a<10  ) tt
            limit 10) t  where a<10 order by 1 ;"""
    qt_pull_up_from_agg """select a from (select a from extend_infer_t3 t1 where a<10 group by a limit 10) t where a<10 order by 1"""
}