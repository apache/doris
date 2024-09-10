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

suite("correlated_scalar_subquery") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
        drop table if exists correlated_scalar_t1;
    """
    sql """
        drop table if exists correlated_scalar_t2;
    """

    sql """
        drop table if exists correlated_scalar_t3;
    """

    sql """
        create table correlated_scalar_t1
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        create table correlated_scalar_t2
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        create table correlated_scalar_t3
                (c1 bigint, c2 bigint)
                ENGINE=OLAP
        DUPLICATE KEY(c1, c2)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
        insert into correlated_scalar_t1 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);
    """
    sql """
        insert into correlated_scalar_t2 values (1,null),(null,1),(1,4), (1,2), (null,3), (2,4), (3,7), (3,9),(null,null),(5,1);
    """
    sql """
        insert into correlated_scalar_t3 values (1,null),(null,1),(1,9), (1,8), (null,7), (2,6), (3,7), (3,9),(null,null),(5,1);
    """

    qt_select_where1 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 < 4) order by c1;"""
    qt_select_where2 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 < 4) order by c1;"""
    qt_select_where3 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select e1 from (select 1 k1) as t lateral view explode_numbers(5) tmp1 as e1 where correlated_scalar_t1.c1 = e1 and correlated_scalar_t1.c2 = e1 order by e1) order by c1;"""
    qt_select_where4 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select col from (select c1 col from correlated_scalar_t2 group by c1 ) tt where correlated_scalar_t1.c1 = tt.col) order by c1;"""
    qt_select_where5 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select col from (select max(c1) over() col from correlated_scalar_t2 ) tt where correlated_scalar_t1.c1 = tt.col) order by c1;"""
    qt_select_where6 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select min(correlated_scalar_t2.c1)  from correlated_scalar_t2 join correlated_scalar_t3 on correlated_scalar_t2.c1 = correlated_scalar_t3.c2 where correlated_scalar_t2.c2 = correlated_scalar_t1.c1) order by c1;"""
    qt_select_where7 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select x from (select count(c1)x from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 order by count(c1))tt) order by c1;"""
    qt_select_where8 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(col) from (select c1 col from correlated_scalar_t2 group by c1 ) tt where correlated_scalar_t1.c1 = tt.col) order by c1;"""
    qt_select_where9 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(col) from (select max(c1) over() col from correlated_scalar_t2) tt where correlated_scalar_t1.c1 = tt.col) order by c1;"""
    qt_select_where10 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(correlated_scalar_t2.c1)  from correlated_scalar_t2 join correlated_scalar_t3 on correlated_scalar_t2.c1 = correlated_scalar_t3.c2 where correlated_scalar_t2.c2 = correlated_scalar_t1.c1) order by c1;"""
    qt_select_where11 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(c1) from correlated_scalar_t2 having correlated_scalar_t1.c1 = count(c1)) order by c1;"""

    qt_select_project1 """select c1, sum((select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7)) from correlated_scalar_t1 group by c1 order by c1;"""
    qt_select_project2 """select c1, sum((select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7)) from correlated_scalar_t1 group by c1 order by c1;"""

    qt_select_join1 """select correlated_scalar_t1.* from correlated_scalar_t1 join correlated_scalar_t2 on correlated_scalar_t1.c1 = correlated_scalar_t2.c2 and correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7);"""
    qt_select_join2 """select correlated_scalar_t1.* from correlated_scalar_t1 join correlated_scalar_t2 on correlated_scalar_t1.c1 = correlated_scalar_t2.c2 and correlated_scalar_t1.c2 > (select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7);"""

    qt_select_having1 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select correlated_scalar_t2.c1 from correlated_scalar_t2  where correlated_scalar_t2.c2 < 4 having correlated_scalar_t1.c1 = correlated_scalar_t2.c1);"""  
    qt_select_having2 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select any_value(correlated_scalar_t2.c1) from correlated_scalar_t2  where correlated_scalar_t2.c2 < 4 having correlated_scalar_t1.c1 = any_value(correlated_scalar_t2.c1));"""

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1);
        """
        exception "correlate scalar subquery must return only 1 row"
    }

    test {
        sql """
              select c1, sum((select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1)) from correlated_scalar_t1 group by c1 order by c1;
        """
        exception "correlate scalar subquery must return only 1 row"
    }

    test {
        sql """
              select correlated_scalar_t1.* from correlated_scalar_t1 join correlated_scalar_t2 on correlated_scalar_t1.c1 = correlated_scalar_t2.c2 and correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1);
        """
        exception "correlate scalar subquery must return only 1 row"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 limit 2);
        """
        exception "limit is not supported in correlated subquery"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select e1 from (select k1 from (select 1 k1 ) as t where correlated_scalar_t1.c1 = k1 ) tt lateral view explode_numbers(5) tmp1 as e1 order by e1);
        """
        exception "access outer query's column before lateral view is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select e1 from (select 1 k1) as t lateral view explode_numbers(5) tmp1 as e1 where correlated_scalar_t1.c1 = e1 having correlated_scalar_t1.c2 = e1 order by e1);
        """
        exception "access outer query's column in two places is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select e1 from (select 1 k1) as t lateral view explode_numbers(5) tmp1 as e1 where correlated_scalar_t1.c1 = e1 or correlated_scalar_t1.c2 = e1 order by e1);
        """
        exception "Unsupported correlated subquery with correlated predicate"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select correlated_scalar_t1.c1 from correlated_scalar_t2);
        """
        exception "access outer query's column in project is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select max(c1) over() from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 order by c1);
        """
        exception "access outer query's column before window function is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select max(correlated_scalar_t1.c1) over() from correlated_scalar_t2 order by c1);
        """
        exception "access outer query's column in project is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select min(correlated_scalar_t2.c1)  from correlated_scalar_t2 join (select correlated_scalar_t3.c1 from correlated_scalar_t3 where correlated_scalar_t1.c1 = correlated_scalar_t3.c2 ) tt on correlated_scalar_t2.c2 > tt.c1);
        """
        exception "access outer query's column before join is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select correlated_scalar_t2.c1 from correlated_scalar_t2 join correlated_scalar_t3 on correlated_scalar_t1.c1 = correlated_scalar_t3.c2 );
        """
        exception "access outer query's column in join is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 order by correlated_scalar_t1.c1);
        """
        exception "Unknown column"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from (select c1 from correlated_scalar_t2 order by correlated_scalar_t1.c1)tt );
        """
        exception "Unknown column"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 group by c2);
        """
        exception "access outer query's column before agg with group by is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 having count(c1) > 10);
        """
        exception "only project, sort and subquery alias node is allowed after agg node"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(correlated_scalar_t1.c1) from correlated_scalar_t2);
        """
        exception "access outer query's column in aggregate is not supported"
    }

    test {
        sql """
              select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select count(col) from (select max(c1) col from correlated_scalar_t2 where correlated_scalar_t1.c1 = c1) tt );
        """
        exception "access outer query's column before two agg nodes is not supported"
    }
}