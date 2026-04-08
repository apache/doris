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

suite("analyze_agg") {
    sql """DROP TABLE IF EXISTS t1"""
    sql """DROP TABLE IF EXISTS t2"""

    sql """
        SET enable_fallback_to_original_planner=false;
        SET enable_nereids_planner=true;
        SET ignore_shape_nodes='PhysicalDistribute';
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;
        """

    sql """    
        create table t1
        (
           id INT,
           a VARCHAR(32)
        )ENGINE = OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        create table t2
        (
            id INT,
            b VARCHAR(30),
            c INT default '0',
            d VARCHAR(30),
            e VARCHAR(32),
            a VARCHAR(32),
            f VARCHAR(32),
            g DECIMAL(9, 3)
        )ENGINE = OLAP
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 30
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    qt_sql """
        
        SELECT
               tt.d,
               tt2.c
        FROM  t1 t
                 LEFT JOIN t2 tt
                           ON tt.f = t.a
                               and tt.b = 'EA'
                 left join t2 tt2
                           on tt2.f = t.a
                               and tt2.b = 'CS'
        group by
                 tt.d,
                 tt2.d,
                 tt2.c;
    """
    sql "select count(distinct t2.b), variance(distinct t2.c) from t2"

    // should not bind g /g in group by again, otherwise will throw exception
    sql "select g / g as nu, sum(c) from t2 group by nu"
    sql """
            select
                1,
                id / (select max(id) from t2)  as 'x',
                count(distinct c) as 'y'
            from
                t2
            group by
                1,
                x
        """

    // check group by expression not contains aggregate function and window expression
    test {
        sql "select SUM(id) FROM t1 group by 1;"
        exception "GROUP BY expression must not contain aggregate functions: sum(id)"
    }

    test {
        // no exception
        sql "select id as k from t1 group by k;"
    }

    test {
        sql "select sum(id) as k from t1 group by k;"
        exception "GROUP BY expression must not contain aggregate functions: sum(id)"
    }

    test {
        sql "select sum(id) as k from t1 group by k + 1;"
        exception " GROUP BY expression must not contain aggregate functions: (sum(id) + 1)"
    }

    test {
        sql "select sum(id) as x, max(id) as y from t1 group by grouping sets((x), (y));"
        exception "GROUP BY expression must not contain aggregate functions: sum(id)"
    }

    test {
        sql "select 100000 as y from t1 group by grouping sets((sum(id)), (max(id)));"
        exception "GROUP BY expression must not contain aggregate functions: sum(id)"
    }

    test {
        sql "select id FROM t1 group by SUM(id);"
        exception "GROUP BY expression must not contain aggregate functions: sum(id)"
    }

    test {
        sql "select SUM(id) OVER() FROM t1 group by 1;"
        exception "GROUP BY expression must not contain window functions: sum(id) OVER()"
    }

    test {
        sql "select SUM(id) OVER() as k FROM t1 group by k;"
        exception "GROUP BY expression must not contain window functions: sum(id) OVER()"
    }

    test {
        sql "select id FROM t1 group by SUM(id) OVER();"
        exception "GROUP BY expression must not contain window functions: sum(id) OVER()"
    }

    // check having
    test {
        sql "select 1234 from t1 having sum(id) over() > 0"
        exception "LOGICAL_HAVING can not contains WindowExpression expression: sum(id) OVER()"
    }

    test {
        sql "select 1234 from t1 group by id having sum(id) over() > 0"
        exception "LOGICAL_HAVING can not contains WindowExpression expression: sum(id) OVER()"
    }

    test {
        sql "select sum(id) over() as k from t1 group by id having sum(id) over() > 0"
        exception "LOGICAL_HAVING can not contains WindowExpression expression: sum(id) OVER()"
    }

    test {
        sql '''SELECT 1 AS a, COUNT(*), SUM(2), AVG(1), RANK() OVER() AS w_rank
            WHERE 1 = 1
            GROUP BY a
            HAVING COUNT(*) IN (1, 2) AND w_rank = 1
            ORDER BY a;
            '''

        exception " HAVING expression 'w_rank' must not contain window functions: rank() OVER()"
    }

    test {
        sql '''SELECT 1 AS a, COUNT(*), SUM(2), AVG(1), RANK() OVER() AS w_rank
            WHERE 1 = 1
            HAVING COUNT(*) IN (1, 2) AND w_rank = 1
            ORDER BY a;
            '''

        exception " HAVING expression 'w_rank' must not contain window functions: rank() OVER()"
    }

    qt_window_1 '''explain  shape plan
        select sum(1) over()
        '''

    qt_window_2 '''explain  shape plan
        select sum(id) over() from t1
        '''

    qt_distinct_1 '''explain  shape plan
        select distinct sum(1) over()
        '''

    qt_distinct_2 '''explain  shape plan
        select distinct sum(sum(1)) over()
        '''

    qt_distinct_3 '''explain  shape plan
        select distinct sum(id) over() from t1
        '''

    qt_distinct_4 '''explain  shape plan
        select distinct sum(sum(id)) over() from t1
        '''

    // having need before windows
    qt_having_with_window_1 '''explain shape plan
        select sum(id) over ()
        from t1
        where id + random(1, 1) > 0
        group by id, id + random(1, 1)
        having sum(id + random(1, 1)) > 1
        order by id + random(1, 1), sum(id + random(1, 1)), sum(id + random(1, 1)) over ()
        '''

    qt_having_with_window_2 '''explain shape plan
        select sum(id) over (partition by a)
        from t1
        where id + random(1, 100) > 0
        group by id, id + random(1, 100), a
        having sum(id + random(1, 100)) > 1
        order by id + random(1, 100), sum(id + random(1, 100)), sum(id + random(1, 100)) over ()
        '''

    qt_having_with_window_3 '''explain shape plan
        select 12345
        from t1
        order by sum(id + random(1, 10)) over ()
        '''

    qt_having_with_window_4 '''explain shape plan
        select distinct id + random(1, 10)
        from t1
        order by sum(id + random(1, 10)) over ()
        '''

    test {
        sql "select id + sum(c) from t2 group by a"
        exception "PROJECT expression 'id' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    explainAndOrderResult 'disable_full_group_by_1', '''
        select /*+ SET_VAR(sql_mode='') */ id + sum(c) from t2 group by a
        '''

    test {
        sql "select a from t2 order by sum(id)"
        exception "PROJECT expression 'a' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    explainAndOrderResult 'disable_full_group_by_2', '''
        select /*+ SET_VAR(sql_mode='') */ a from t2 order by sum(id)
        '''

    test {
        sql "select 1000 from t2 having count(c) > 10 order by sum(id), a"
        exception "SORT expression 'a' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    explainAndOrderResult 'disable_full_group_by_3', '''
        select /*+ SET_VAR(sql_mode='') */ 1000 from t2 having count(c) > 10 order by sum(id), a
        '''

    test {
        sql "select 1000 from t2 having count(c) > 10 order by a"
        exception "SORT expression 'a' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    explainAndOrderResult 'disable_full_group_by_4', '''
        select /*+ SET_VAR(sql_mode='') */ 1000 from t2 having count(c) > 10 order by a
        '''

    test {
        sql "select 1000 from t2 having c > 10 order by sum(id)"
        exception "HAVING expression 'c' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    explainAndOrderResult 'disable_full_group_by_5', '''
        select /*+ SET_VAR(sql_mode='') */ 1000 from t2 having c > 10 order by sum(id)
        '''

    test {
        // when generate an aggregate, havig also need to check slots for group by
        sql "select 1000 from t2 having id > 0 and count(*) > 0"
        exception "HAVING expression 'id' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    // when not generate an aggregate, having can treat like a filter, then no check in group by list error
    sql "select 1000 from t2 having id > 0"

    // check sort -> having -> project generate aggregate
    test {
        // check project fail
        sql "select a from t2 having count(c) > 10 order by sum(id)"
        exception "PROJECT expression 'a' must appear in the GROUP BY clause or be used in an aggregate function"
    }
    // check project ok
    sql "select 1000 from t2 having count(c) > 10 order by sum(id)"
    sql "select 1000 as k from t2 having count(c) > 10 order by sum(id)"

    explainAndOrderResult 'disable_full_group_by_6', '''
        select /*+ SET_VAR(sql_mode='') */ a + id from t2 having count(c) > 10 order by sum(id)
        '''

    explainAndOrderResult 'sort_project_1', '''
        select 1 from t2 order by sum(id);
        '''

    explainAndOrderResult 'sort_having_project_1', '''
        select 1 from t2 having count(c) > 10 order by sum(id);
        '''

    sql "drop table if exists tbl_analyze_agg_3"
    sql "create table tbl_analyze_agg_3 (a bigint, b bigint, c int) distributed by hash(a) properties('replication_num'='1');"
    sql """
    INSERT INTO tbl_analyze_agg_3 VALUES
    (1, NULL, 3), (2, NULL, 5), (3, NULL, 7),
    (4,5,6),(4,5,7),(4,5,8),
    (5,0,0),(5,0,0),(5,0,0); 
    """
    qt_test_sum0 "select sum0(distinct b),sum(distinct c) from tbl_analyze_agg_3 group by a order by 1,2"
    qt_test_sum0_all_null "select sum0(distinct b),sum(distinct c) from tbl_analyze_agg_3 where a in (1,2,3) group by a order by 1,2"

    explainAndResult 'order_window_1', '''
        select a, b
        from tbl_analyze_agg_3
        order by sum(a + b) over (partition by b), rank() over(), a, b
        '''

    explainAndResult 'order_window_2', '''
        select a, b, sum(a + b) over (partition by b), rank() over(), max(a) over()
        from tbl_analyze_agg_3
        order by sum(a + b) over (partition by b), rank() over(), a, b
        '''

    //========================================================
    // test for bind expression and no full group by
    explainAnalyzedPlan  'bind_expr_1',  '''
        select a
        from tbl_analyze_agg_3
        order by b, a + b
    '''
}
