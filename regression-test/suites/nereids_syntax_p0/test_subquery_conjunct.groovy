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

suite("test_subquery_conjunct") {
    sql "set enable_nereids_planner=true"
    sql """drop table if exists subquery_conjunct_table;"""
    sql """CREATE TABLE `subquery_conjunct_table` (
            `id` INT NOT NULL,
            `c1` INT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `c1`)
            DISTRIBUTED BY RANDOM BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

    sql """insert into subquery_conjunct_table values(1, 1),(2,2),(-1,-1),(-2,-2),(2,1),(3,2);"""
    qt_select_simple_scalar """select * from subquery_conjunct_table t1 where abs(t1.c1) != (select sum(c1) from subquery_conjunct_table t2 where t2.c1 + t2.id = t1.c1) order by t1.id, t1.c1;"""
    qt_select_complex_scalar """select * from subquery_conjunct_table t1 where abs(t1.c1) != (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1 + t2.id) = t1.c1) order by t1.id, t1.c1;"""
    qt_select_simple_in """select * from subquery_conjunct_table t1 where abs(t1.c1) in (select c1 from subquery_conjunct_table t2 where t2.c1 + t2.id -1  = t1.c1) order by t1.id, t1.c1;"""
    qt_select_complex_in """select * from subquery_conjunct_table t1 where abs(t1.c1) in (select c1 from subquery_conjunct_table t2 where abs(t2.c1+ t2.id -1) = t1.c1) order by t1.id, t1.c1;"""
    qt_select_simple_not_in """select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select c1 from subquery_conjunct_table t2 where t2.c1 + t2.id = t1.c1) order by t1.id, t1.c1;"""
    qt_select_complex_not_in """select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select c1 from subquery_conjunct_table t2 where abs(t2.c1 + t2.id) = t1.c1) order by t1.id, t1.c1;"""
    qt_select_simple_exists """select * from subquery_conjunct_table t1 where exists (select c1 from subquery_conjunct_table t2 where t2.c1 + t2.id = t1.c1) order by t1.id, t1.c1;"""
    qt_select_complex_exists """select * from subquery_conjunct_table t1 where exists (select c1 from subquery_conjunct_table t2 where abs(t2.c1 + t2.id) = t1.c1) order by t1.id, t1.c1;"""
    qt_select_simple_not_exists """select * from subquery_conjunct_table t1 where not exists (select c1 from subquery_conjunct_table t2 where t2.c1 + t2.id = t1.c1) order by t1.id, t1.c1;"""
    qt_select_complex_not_exists """select * from subquery_conjunct_table t1 where not exists (select c1 from subquery_conjunct_table t2 where abs(t2.c1 + t2.id) = t1.c1) order by t1.id, t1.c1;"""
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) != (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1) - t1.c1 = 0) order by t1.id; """
        exception "Unsupported correlated subquery with correlated predicate"
    }
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) != ( select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1 -1) + t1.id = t1.c1) order by t1.id, t1.c1; """
        exception "Unsupported correlated subquery with correlated predicate"
    }
    // a scalar subquery whose correlated predicate is not an equality is evaluated on the
    // aggregation of the domain of every outer row: the value of a sum over an empty domain is null
    // (it is not the null of the left outer join, which the aggregation of the rewrite produces
    // itself), and the outer row is kept when its value differs from that null
    qt_select_ne_sum_gt """ select * from subquery_conjunct_table t1 where abs(t1.c1) != (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1) > t1.c1) order by t1.id; """
    // IN over the aggregation of the correlated rows, with a non trivial expression on the left
    // hand side of the IN: the expression is compared with the value of the select list of the
    // subquery, and a row whose domain is empty compares with the aggregation of an empty domain
    // (NULL for sum) instead of being dropped or compared with nothing
    qt_select_in_sum """ select * from subquery_conjunct_table t1 where abs(t1.c1) in (select sum(c1) from subquery_conjunct_table t2 where t2.c1 + 1 = t1.c1) order by t1.id, t1.c1; """
    qt_select_in_sum_abs """ select * from subquery_conjunct_table t1 where abs(t1.c1) in (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1) = t1.c1) order by t1.id, t1.c1; """
    qt_select_not_in_sum """ select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select sum(c1) from subquery_conjunct_table t2 where t2.c1 + 1= t1.c1) order by t1.id, t1.c1; """
    qt_select_not_in_sum_abs """ select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1 -1) = t1.c1) order by t1.id, t1.c1; """
    // a grouped scalar subquery whose select list computes its value (count(*) + 1): the projection
    // which computes the value sits between the aggregation of the subquery and the aggregation which
    // counts the rows of a correlation key (the runtime check of a scalar subquery), so the rewrite
    // has to carry it. The value is computed for the domain of every outer row, and the outer rows
    // whose domain has no group return null
    order_qt_computed_grouped_scalar """
        select t1.id, t1.c1, (select count(*) + 1 from subquery_conjunct_table t2 where t2.id = t1.id group by t2.id) as v
        from subquery_conjunct_table t1 order by t1.id, t1.c1;
    """
    order_qt_computed_grouped_scalar_empty_domain """
        select t1.id, t1.c1, (select count(*) + 1 from subquery_conjunct_table t2 where t2.id = t1.id + 100 group by t2.id) as v
        from subquery_conjunct_table t1 order by t1.id, t1.c1;
    """
    // the domain of an outer row holds several groups of the derived table, so the value of the
    // subquery is not unique for that row: the runtime check of the scalar subquery rejects the query
    test {
        sql """ select t1.id, (select count(*) + 1 from subquery_conjunct_table t2 where t2.id = t1.id group by t2.c1) as v
            from subquery_conjunct_table t1 order by t1.id; """
        exception "correlate scalar subquery must return only 1 row"
    }
}