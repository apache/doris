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
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) != (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1) > t1.c1) order by t1.id; """
        exception "scalar subquery's correlatedPredicates's operator must be EQ"
    }
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) in (select sum(c1) from subquery_conjunct_table t2 where t2.c1 + 1 = t1.c1) order by t1.id, t1.c1; """
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) in (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1) = t1.c1) order by t1.id, t1.c1; """
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select sum(c1) from subquery_conjunct_table t2 where t2.c1 + 1= t1.c1) order by t1.id, t1.c1; """
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql """ select * from subquery_conjunct_table t1 where abs(t1.c1) not in (select sum(c1) from subquery_conjunct_table t2 where abs(t2.c1 -1) = t1.c1) order by t1.id, t1.c1; """
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
}