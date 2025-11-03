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

suite('test_subquery_nullable') {
    sql 'set enable_parallel_result_sink=false;'
    sql 'DROP TABLE IF EXISTS test_subquery_nullable_t1 FORCE'
    sql 'DROP TABLE IF EXISTS test_subquery_nullable_t2 FORCE'
    sql "CREATE TABLE test_subquery_nullable_t1(a int not null, b int not null, c int not null) distributed by hash(a) properties('replication_num' = '1')"
    sql "CREATE TABLE test_subquery_nullable_t2(x int not null, y int not null, z int not null) distributed by hash(x) properties('replication_num' = '1')"
    sql 'INSERT INTO test_subquery_nullable_t1 values(1, 1, 1), (2, 2, 2)'
    sql 'INSERT INTO test_subquery_nullable_t2 values(1, 1, 1), (2, 2, 2)'
    sql "SET detail_shape_nodes='PhysicalProject'"
    order_qt_uncorrelate_scalar_subquery '''
        with cte1 as (select a, (select x from test_subquery_nullable_t2 where x > 1000 limit 1) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    qt_uncorrelate_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select x from test_subquery_nullable_t2 where x > 1000 limit 1) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    order_qt_correlate_scalar_subquery '''
        with cte1 as (select a, (select x from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y limit 1) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    qt_correlate_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select x from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y limit 1) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    order_qt_uncorrelate_top_nullable_agg_scalar_subquery '''
        with cte1 as (select a, (select sum(x) from test_subquery_nullable_t2 where x > 1000) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    qt_uncorrelate_top_nullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select sum(x) from test_subquery_nullable_t2 where x > 1000) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1
    '''
    order_qt_correlate_top_nullable_agg_scalar_subquery '''
        with cte1 as (select a, (select sum(x) from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    qt_correlate_top_nullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select sum(x) from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    order_qt_uncorrelate_top_notnullable_agg_scalar_subquery '''
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    qt_uncorrelate_top_notnullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    order_qt_correlate_top_notnullable_agg_scalar_subquery '''
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    qt_correlate_top_notnullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 and test_subquery_nullable_t1.b = test_subquery_nullable_t2.y) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    order_qt_uncorrelate_notop_notnullable_agg_scalar_subquery '''
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 group by x) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    qt_uncorrelate_notop_notnullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 group by x) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    order_qt_correlate_notop_notnullable_agg_scalar_subquery '''
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 group by x having test_subquery_nullable_t1.a + test_subquery_nullable_t1.b = test_subquery_nullable_t2.x) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    qt_correlate_notop_notnullable_agg_scalar_subquery_shape '''explain shape plan
        with cte1 as (select a, (select count(x) from test_subquery_nullable_t2 where x > 1000 group by x having test_subquery_nullable_t1.a + test_subquery_nullable_t1.b = test_subquery_nullable_t2.x) as x from test_subquery_nullable_t1)
        select a, x > 10 and x < 1 from cte1;
    '''
    sql 'DROP TABLE IF EXISTS test_subquery_nullable_t1 FORCE'
    sql 'DROP TABLE IF EXISTS test_subquery_nullable_t2 FORCE'
}
