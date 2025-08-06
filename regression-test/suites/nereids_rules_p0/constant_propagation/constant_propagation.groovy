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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
suite('constant_propagation') {
    def explain_and_result = { tag, sql ->
        "qt_${tag}_shape"          "explain shape plan ${sql}"
        "order_qt_${tag}_result"   "${sql}"
    }

    def explain_and_update = { tag, s ->
        "qt_${tag}_shape"          "explain shape plan ${s}"
        sql                        "${s}"
        "order_qt_${tag}_result"   'select * from t3'
    }

    multi_sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET detail_shape_nodes='PhysicalProject,PhysicalHashAggregate,PhysicalQuickSort';
        SET ignore_shape_nodes='PhysicalDistribute';
        SET runtime_filter_type=2;
        """

    sql 'drop table if exists t1 force'
    sql 'drop table if exists t2 force'
    sql 'drop table if exists t3 force'

    sql """
    create table t1(a int, b int, c int, d int, e int, f int,
                    d_int int, d_smallint smallint, d_tinyint tinyint,
                    d_char100 char(100),
                    d_char10 char(10),
                    d_string string,
                    d_varchar varchar(20),
                    d_datetimev2 datetimev2,
                    d_datev2 datev2,
                    d_date date,
                    d_datetime datetime,
                    d_datetime3 datetimev2(3)
                   )
    properties('replication_num'='1');
    """

    sql """
    create table t2(x int, y int, z int, u int, v int, w int,
                    d_int int, d_smallint smallint, d_tinyint tinyint,
                    d_char100 char(100),
                    d_char10 char(10),
                    d_string string,
                    d_varchar varchar(20),
                    d_datetimev2 datetimev2,
                    d_datev2 datev2,
                    d_date date,
                    d_datetime datetime,
                    d_datetime3 datetimev2(3)
                   )
    properties('replication_num'='1');
    """

    sql """
    create table t3(a int, b int, c smallint, d bigint)
    unique key(a)
    distributed by hash(a) buckets 10
    properties('replication_num'='1');
    """

    sql """
    insert into t1 values(1, 2, 3, 4, 5, 6,
                           1, 3, 3,
                           '0123', '01234', '012345', '0123456',
                           '2020-01-09 10:00:00.99','2020-01-09','2022-08-09','2022-08-09 10:00:00', '2020-10-10 01:02:03.456'),
                         (10, 20, 30, 40, 50, 60,
                           14,33,23,
                           '9876', '98765', '987654', '9876543',
                           '2020-01-11 10:00:00.99','2020-01-11','2022-08-03','2022-08-09 10:00:02', '2020-10-10 01:02:03.456');
    """

    sql '''
    insert into t2 select * from t1
    '''

    sql '''
    insert into t3 values(1, 10, 100, 1000), (2, 20, 200, 2000), (3, 30, 300, 3000)
    '''

    multi_sql'''
        drop table if exists s1 force;
        drop table if exists s2 force;
        drop table if exists s3 force;
        create table s1(c1 int, c2 int) properties ('replication_num' = '1');
        create table s2(c1 int, c2 int) properties ('replication_num' = '1');
        create table s3(c1 int, c2 int) properties ('replication_num' = '1');
        insert into s1 values (1,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4);
        insert into s2 values (1,4), (1,2),  (2,4), (3,7), (3,9),(5,1);
        insert into s3 values (1,9), (1,8),  (2,6), (3,7), (3,9),(5,1);
    '''

    explain_and_result 'string_1',  """
        select d_char10, concat("hello", d_char10), d_char100, concat("hello", d_char100), d_varchar, concat("hello", d_varchar), d_string, concat("hello", d_string)
        from t1
        where d_char10 = 'a' and d_char100 = concat(d_char10, 'b') and d_varchar = concat(d_char100, 'c') and d_string = concat(d_varchar, 'd')
    """

    explain_and_result 'datetime_1', """
        select date_add(d_date, interval 1 day), date_add(d_datetimev2, interval 1 DAY), date_add(d_datetime3, interval 1 DAY)
        from t1
        where d_date = '2023-01-01'
        and date_add(d_date, INTERVAL 1 DAY) = '2023-01-02' and  d_datetimev2 = '2020-10-10 01:02:03' and d_datetime3 = '2020-10-10 01:02:03.456'
    """

    explain_and_result 'project_1', '''
       select d_int, d_int as a,  (d_int + 10) * 2
       from t1
       where d_int = 10
    '''

    explain_and_result 'project_2', '''
        select d_int, (10 + 5) * d_int, d_int * (20 - 10)
        from t1
        where d_int = 15 * 2
    '''

    explain_and_result 'filter_1', '''
        select 1
        from t1
        where a = 1 and a + b = 3 and b + c = 5 and c + d = 7
    '''

    explain_and_result 'sort_1', '''
        select a, b, c
        from t1
        where a = 1 and b = 2 and c = 3
        order by  a + b, b + c;
    '''

    explain_and_result 'sort_2', '''
        select a, b, c
        from t1
        where a = 1 and b = 2
        order by  a + b, b + c;
    '''

    explain_and_result 'sort_3', '''
        select a, b, c
        from t1
        where b = 2 and c = 3
        order by  a + b, b + c;
    '''

    explain_and_result 'sort_4', '''
        select a, b, c
        from t1
        where a = 1
        order by 1;
    '''

    explain_and_result 'join_1', '''
       select a, b, c, x, y, z
       from t1, t2
       where a = 1 and a + b = 2 and b = x + y
    '''

    explain_and_result 'join_2', '''
       select a, x
       from t1 left outer join t2 on t1.a = 1 and t2.x = 2 and t1.a > t2.x
    '''

    explain_and_result 'join_3', '''
       select a, x
       from t1 full outer join t2 on t1.a = 1 and t2.x = 2 and t1.a > t2.x
    '''

    explain_and_result 'join_4', '''
       select a, x
       from t1 join t2 on t1.a = t2.x * 10 and t2.x = 1
    '''

    explain_and_result 'join_5', '''
       select a, x
       from t1 left join t2 on t1.a = t2.x * 10 and t2.x = 1
    '''

    explain_and_result 'join_6', '''
       select a, x
       from t1 full outer join t2 on t1.a = t2.x * 10 and t2.x = 1
    '''

    explain_and_result 'join_7', '''
       select a
       from t1 left semi join t2 on t1.a = t2.x * 10 and t2.x = 1
    '''

    explain_and_result 'join_8', '''
       select a
       from t1 left anti join t2 on t1.a = t2.x * 10 and t2.x = 1
    '''

    explain_and_result 'join_9', '''
       select t1.a, t2.x, t3.a
       from t1 join t2 on t1.a = t2.x * 10 join t3 on t2.x = t3.a where t1.a = 10
    '''

    explain_and_result 'join_10', '''
        select t1.a
        from t1 where t1.a in (select t3.a from t3 where t3.b = 10);
    '''

    explain_and_result 'join_11', '''
        select t1.a
        from t1 where t1.a not in (select t3.a from t3 where t3.b = 10);
    '''

    /* // BUG start
    // BUG: the result should  '[[null], [null]]', but be return '[[true], [true]]'
    explain_and_result  'join_12', '''
         select 1  in (select null from t2)
         from t1;
    '''

    // BUG: the result should  '[[null], [null]]', but be return '[[false], [false]]'
    explain_and_result  'join_13', '''
        select 1 not in (select null from t2)
        from t1;
    '''

    // OK
    explain_and_result 'join_14', '''
        select null  in (select c1 from s2) from s1
    '''

    // BUG: the result expect multi rows with null, but be return multi rows with true
    explain_and_result 'join_15', '''
        select null  in (select 1 from s2) from s1
    '''

    // BUG: the result expect multi rows with null, but be return multi rows with true
    explain_and_result 'join_16', '''
        select 1  in (select null from s2) from s1
    '''

    // BUG: the result expect multi rows with null, but be return multi rows with false
    explain_and_result 'join_17', '''
        select 1 not in (select null from s2) from s1
    '''
    */ // BUG end

    explain_and_result 'subquery_1', '''
       select a, x
       from (select a from t1 where a = 1) s1, (select x from t2 where x = 1) s2
       where a < x;
    '''

    explain_and_result 'subquery_2', '''
       select a, x
       from (select a from t1 where a = 1) s1, (select x from t2) s2
       where a = x;
    '''

    explain_and_result 'subquery_3', '''
      select a, b, c
      from t1
      where t1.a = 1 and exists (select * from t2 where t2.x = 1 and t1.a > t2.x)
    '''

    explain_and_result 'subquery_4', '''
      select a, b, c
      from t1
      where t1.a = 1 and exists (select * from t2 where t1.a = t2.x)
    '''

    explain_and_result 'subquery_5', '''
      select a, b, c
      from t1
      where t1.a > 10 and t1.a in (select t2.x + t2.y from t2 where t2.x = 1 and t2.y = 2)
    '''

    explain_and_result 'subquery_6', '''
      select a, b, c
      from t1
      where t1.a + 2 in (select t2.x + t2.y from t2 where t2.x = 1 and t2.y = 2)
    '''

    explain_and_result 'subquery_7', '''
      select t.a, t2.x
      from (select * from t1 where a = 10) t join t2 on t.a = t2.x * 10
    '''

    explain_and_result 'subquery_8', '''
      select t.k, t.b, t3.a
      from (select a * 10 as k, b from t1 union all select x as k, y as b from t2) t join t3 on t.k = t3.a * 5 where t3.a = 2
    '''

    explain_and_result 'subquery_9', '''
      select t.k, t.b, t3.a
      from (select a * 10 as k, b from t1 except select x as k, y as b from t2) t join t3 on t.k = t3.a * 5 where t3.a = 2
    '''

    explain_and_result 'subquery_10', '''
      select t.k, t.b, t3.a
      from (select a * 10 as k, b from t1 intersect select x as k, y as b from t2) t join t3 on t.k = t3.a * 5 where t3.a = 2
    '''

    explain_and_result 'subquery_10', '''
      select t1.a
      from t1 where t1.a in (select t2.x * 10 from t2 where t2.x = 1)
    '''

    explain_and_result 'subquery_11', '''
      select t1.a
      from t1 where t1.a in (select t2.x - 10 from t2 where t2.x = 20)
    '''

    explain_and_result 'subquery_12', '''
      select t1.a
      from t1 where exists (select 0 from t2 where t2.x = 1 and t1.a = t2.x * 10)
    '''

    explain_and_result 'subquery_13', '''
      select t1.a
      from t1 where exists (select 0 from t2 where t2.x = 20 and t1.a + 10 = t2.x)
    '''

    explain_and_result 'subquery_14', '''
      select t1.a
      from t1 where not exists (select 0 from t2 where t2.x = 1 and t1.a = t2.x * 10)
    '''

    explain_and_result 'subquery_15', '''
      select t1.a
      from t1 where not exists (select 0 from t2 where t2.x = 20 and t1.a + 10 = t2.x)
    '''

    explain_and_result 'agg_1', '''
        select a + b, (a + b) + 10 as k
        from t1
        where a = 1 and b = 2
        group by a + b
    '''

    explain_and_result 'agg_2', '''
        select a, b
        from t1
        where a = 1
        group by a, b
    '''

    explain_and_result 'agg_3', '''
        select a, b
        from t1
        where a = 1 and b = 2
        group by a, b
    '''

    explain_and_result 'agg_4', '''
        select a, b, a + b as k, c
        from t1
        where a = 1 and b = 2
        group by a, b, c
        having k > 0;
    '''

    explain_and_result 'agg_5', '''
        select a, b, a + b as k, c
        from t1
        where a = 1 and b = 2
        group by a, b, c
        having k > 10;
    '''

    explain_and_result 'agg_6', '''
        select a, count(a)
        from t1
        where a = 10
        group by a
        having sum(b) > a
    '''

    explain_and_result 'agg_7', '''
        select a, b, count(a)
        from t1
        where a = 10 and b = 20
        group by a, b
        having sum(b) > a and a > b;
    '''

    test {
        sql 'select a, b, a + b, c from t1 where a = 1 and b = 2 group by a, b'
        exception "c not in aggregate's output"
    }

    explain_and_result 'union_1', '''
        select 1 union select 1
    '''

    explain_and_result 'union_2', '''
        select 1 union all select 1
    '''

    explain_and_result 'union_3', '''
        select *
        from (
            (select a + b as k1, a - b as k2, c as k3 from t1 where a = 1 and b = 2)
            union
            (select x + y as k1, x - y as k2, z as k3 from t2 where x = 1 and y = 2)
            ) t
        where k1 + k2 + k3 = 5;
    '''

    explain_and_result 'union_4', '''
        select *
        from (
            (select a + b as k1, a - b as k2, c as k3 from t1 where a = 1 and b = 2)
            union all
            (select x + y as k1, x - y as k2, z as k3 from t2 where x = 1 and y = 2)
            ) t
        where k1 + k2 + k3 = 5;
    '''

    explain_and_result 'union_5', '''
        select *
        from (
            (select a + b as k1, a - b as k2, c as k3 from t1)
            union
            (select x + y as k1, x - y as k2, z as k3 from t2)
            ) t
        where k1 = 3 and  k2 = -1 and k3 = 3;
    '''

    explain_and_result 'union_6', '''
        select *
        from (
            (select a + b as k1, a - b as k2, c as k3 from t1)
            union all
            (select x + y as k1, x - y as k2, z as k3 from t2)
            ) t
        where k1 = 3 and  k2 = -1 and k3 = 3;
    '''

    explain_and_result 'union_7', '''
        select *
        from (select 10 as a union select 10 as a) t
        where a = 10
    '''

    explain_and_result 'union_8', '''
        select *
        from (select 10 as a union all select 10 as a) t
        where a = 10
    '''

    explain_and_result 'union_9', '''
        select *
        from (select 10 as a union select a as a from t1 where a = 10) t
        where a = 10
    '''

    explain_and_result 'union_10', '''
        select *
        from (select 10 as a union all select a as a from t1 where a = 10) t
        where a = 10
    '''

    explain_and_result 'union_11', '''
        select *
        from (select 10 as a union select b + 1 as a from t1 where b = 9 union select 9 + 1) t
        where a = 10
    '''

    explain_and_result 'union_12', '''
        select *
        from (select 10 as a union all select b + 1 as a from t1 where b = 9 union select 9 + 1) t
        where a = 10
    '''

    explain_and_result 'union_13', '''
        select *
        from (select 2 as a union select a as a from t1) t
        where a = 1
    '''

    explain_and_result 'union_14', '''
        select *
        from (select 2 as a union all select a as a from t1) t
        where a = 1
    '''

    explain_and_result 'union_15', '''
        select *
        from (select 1 as a union select a as a from t1) t
        where a = 1
    '''

    explain_and_result 'union_16', '''
        select *
        from (select 1 as a union all select a as a from t1) t
        where a = 1
    '''

    explain_and_result 'union_17', '''
       select 1, 3 union select 0 + 1, 1 + 2 union select a, a + b from t1 where a = 1 and b = 2;
    '''

    explain_and_result 'union_18', '''
       select 1, 3 union select 0 + 1, 1 + 2 union all select a, a + b from t1 where a = 1 and b = 2;
    '''

    explain_and_result 'union_19', '''
       select * from (select 10 as a union all select b + 1 as a from t1 where b = 9) t
    '''

    explain_and_result 'intersect_1', '''
        select a from t1 where a = 1 intersect select x + 1 from t2 where x = 1
    '''

    explain_and_result 'intersect_2', '''
        select *
        from (select a, a + b as k from t1 where a = 1 intersect select x, x + y as k from t2 where x = 1) t
        where k = 3
    '''

    explain_and_update 'update_1', '''
        update t3 set b = a + b, c = a + b, d = a + 10
        where a = 1
    '''

    explain_and_update 'update_2', '''
        update t3 set b = a, c = a, d = a
        where a = 1
    '''

    explain_and_result 'no_replace_1', '''
        select count(1) from t1 where d_string = '012345' and d_string match_any '012345';
    '''
}
