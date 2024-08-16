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
        insert into correlated_scalar_t1 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);
    """
    sql """
        insert into correlated_scalar_t2 values (1,null),(null,1),(1,4), (1,2), (null,3), (2,4), (3,7), (3,9),(null,null),(5,1);
    """

    qt_select_where1 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 < 4);"""
    qt_select_where2 """select c1 from correlated_scalar_t1 where correlated_scalar_t1.c2 > (select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 < 4);"""

    qt_select_project1 """select c1, sum((select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7)) from correlated_scalar_t1 group by c1 order by c1;"""
    qt_select_project2 """select c1, sum((select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7)) from correlated_scalar_t1 group by c1 order by c1;"""

    qt_select_join1 """select correlated_scalar_t1.* from correlated_scalar_t1 join correlated_scalar_t2 on correlated_scalar_t1.c1 = correlated_scalar_t2.c2 and correlated_scalar_t1.c2 > (select c1 from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7);"""
    qt_select_join2 """select correlated_scalar_t1.* from correlated_scalar_t1 join correlated_scalar_t2 on correlated_scalar_t1.c1 = correlated_scalar_t2.c2 and correlated_scalar_t1.c2 > (select any_value(c1) from correlated_scalar_t2 where correlated_scalar_t1.c1 = correlated_scalar_t2.c1 and correlated_scalar_t2.c2 > 7);"""

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
}
