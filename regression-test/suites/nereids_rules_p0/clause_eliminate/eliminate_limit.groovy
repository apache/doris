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

suite("eliminate_limit") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """DROP TABLE IF EXISTS t1;"""
    sql """DROP TABLE IF EXISTS t2;"""

    sql """
    CREATE TABLE IF NOT EXISTS t1(
      `c1` int(11) NULL,
      `c2` int(11) NULL,
      `c3` int(11) NULL,
      `c4` int(11) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t2(
      `c1` int(11) NULL,
      `c2` int(11) NULL,
      `c3` int(11) NULL,
      `c4` int(11) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """INSERT INTO t1 VALUES (null,null,null,null);"""
    sql """INSERT INTO t1 VALUES (1,1,1,1);"""
    sql """INSERT INTO t1 VALUES (2,2,2,2);"""
    sql """INSERT INTO t1 VALUES (3,3,3,3);"""

    sql """INSERT INTO t2 VALUES (null,null,null,null);"""
    sql """INSERT INTO t2 VALUES (1,1,1,1);"""
    sql """INSERT INTO t2 VALUES (2,2,2,2);"""
    sql """INSERT INTO t2 VALUES (3,3,3,3);"""

    // 1. single table + order by exist columns + different columns
    qt_1_1 """select c1 from t1 order by 1 limit 0;"""
    qt_1_2 """select c2 from t1 order by 1 limit 0;"""
    qt_1_3 """select c1, c2 from t1 order by 2 limit 0;"""

    // 2. multi table + union all + order by exist columns + different columns
    qt_2_1 """select c1 from t1 order by 1 union all select c2 from t2 order by 1 limit 0;"""
    qt_2_2 """(select c1 from t1 order by 1 union all select c2 from t2 order by 1) order by 1 limit 0;"""
    qt_2_3 """(select c2 from t1 order by 1 union all select c1 from t2 order by 1) order by 1 limit 0;"""

    qt_2_4 """(select c2, c1 from t1 order by 1 union all select c1, c2 from t2 order by 1) order by 1 limit 0;"""
    qt_2_5 """(select c2, c3 from t1 order by 1 union all select c1, c2 from t2 order by 1) order by 1 limit 0;"""
    qt_2_6 """(select c2, c1 from t1 order by 1 union all select c1, c2 from t2 order by 1) order by 2 limit 0;"""

    // 3. join + order by exist columns + different columns
    qt_3_1 """select t1.c1 from t1 join t2 on t1.c1 = t2.c2 order by 1 limit 0;"""
    qt_3_2 """select t1.c2 from t1 join t2 on t1.c1 = t2.c2 order by 1 limit 0;"""
    qt_3_3 """select t1.c1, t2.c2 from t1 join t2 on t1.c1 = t2.c2 order by 2 limit 0;"""

    // 4. single table + order by exist columns + in/exist subquery
    qt_4_1 """select * from t1 where t1.c1 in (select c1 from t1 order by 1) limit 0;"""
    qt_4_2 """select * from t1 where t1.c1 in (select c2 from t1 order by 1) limit 0;"""
    qt_4_3 """select * from t1 where t1.c1 in (select c1, c2 from t1 order by 2) limit 0;"""
    qt_4_4 """select * from t1 where exists (select c1 from t1 order by 1) limit 0;"""
    qt_4_5 """select * from t1 where exists (select c2 from t1 order by 1) limit 0;"""
    qt_4_6 """select * from t1 where exists (select c1, c2 from t1 order by 2) limit 0;"""

    // 5. single table + multi subqueries group by not exist columns + different aggregate function + having
    qt_5_1 """select * from t1 where t1.c1 in (select t1.c1 from t1 join t2 on t1.c1 = t2.c2 order by 1) limit 0;"""
    qt_5_2 """select * from t1 where t1.c1 in (select t1.c2 from t1 join t2 on t1.c1 = t2.c2 order by 1) limit 0;"""
    qt_5_3 """select * from t1 where t1.c1 in (select t1.c1, t2.c2 from t1 join t2 on t1.c1 = t2.c2 order by 2) limit 0;"""
    qt_5_4 """select * from t1 where exists (select t1.c1 from t1 join t2 on t1.c1 = t2.c2 order by 1) limit 0;"""
    qt_5_5 """select * from t1 where exists (select t1.c2 from t1 join t2 on t1.c1 = t2.c2 order by 1) limit 0;"""
    qt_5_6 """select * from t1 where exists (select t1.c1, t2.c2 from t1 join t2 on t1.c1 = t2.c2 order by 2) limit 0;"""

    // 1. single table + group by exist columns + different aggregate function + having
    qt_6_1 """select sum(c1) from t1 group by 1 limit 0;"""
    qt_6_2 """select count(c1) from t1 group by 1 limit 0;"""
    qt_6_3 """select avg(c1) from t1 group by 1 limit 0;"""

    qt_6_4 """select sum(c1) from t1 group by 1 having sum(c1) > 2 limit 0;"""
    qt_6_5 """select count(c1) from t1 group by 1 having count(c1) > 2 limit 0;"""
    qt_6_6 """select avg(c1) from t1 group by 1 having avg(c1) > 2 limit 0;"""

    // 2. single table + group by not exist columns + different aggregate function + having
    test {
        sql """select sum(c1) from t1 group by 2 limit 0;"""
        exception "errCode"
    }
    test {
        sql """select count(c1) from t1 group by 2 limit 0;"""
        exception "errCode"
    }
    test {
        sql """select avg(c1) from t1 group by 2 limit 0;"""
        exception "errCode"
    }
    test {
        sql """select sum(c1) from t1 group by 2 having sum(c1) > 2 limit 0;"""
        exception "errCode"
    }
    test {
        sql """select count(c1) from t1 group by 2 having count(c1) > 2 limit 0;"""
        exception "errCode"
    }
    test {
        sql """select avg(c1) from t1 group by 2 having avg(c1) > 2 limit 0;"""
        exception "errCode"
    }

    // 3. join + group by exist columns + different aggregate function + having
    qt_8_1 """select sum(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 limit 0;"""
    qt_8_2 """select count(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 limit 0;"""
    qt_8_3 """select avg(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 limit 0;"""

    qt_8_4 """select sum(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 having sum(c1) > 2 limit 0;"""
    qt_8_5 """select count(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 having count(c1) > 2 limit 0;"""
    qt_8_6 """select avg(c1) from t1 join t2 on t1.c1 = t2.c2 group by 1 having avg(c1) > 2 limit 0;"""

    // 4. single table + group by exist columns + different aggregate function + having + in/exist subquery
    qt_9_1 """select * from t1 where t1.c1 in (select sum(c1) from t1 group by 1) limit 0;"""
    qt_9_2 """select * from t1 where t1.c1 in (select count(c1) from t1 group by 1;) limit 0;"""
    qt_9_3 """select * from t1 where t1.c1 in (select avg(c1) from t1 group by 1) limit 0;"""
    qt_9_4 """select * from t1 where exists (select sum(c1) from t1 group by 1) limit 0;"""
    qt_9_5 """select * from t1 where exists (select count(c1) from t1 group by 1) limit 0;"""
    qt_9_6 """select * from t1 where exists (select avg(c1) from t1 group by 1) limit 0;"""

    // 5. single table + multi subqueries group by not exist columns + different aggregate function + having
    qt_10_1 """select * from t1 where t1.c1 in (select sum(c1) from t1 group by 2 having sum(c1) > 2) and t1.c2 in (select sum(c1) from t1 group by 2 having sum(c1) > 2) limit 0;"""
    qt_10_2 """select * from t1 where t1.c1 in (select count(c1) from t1 group by 2 having count(c1) > 2) and t1.c2 in (select count(c1) from t1 group by 2 having count(c1) > 2) limit 0;"""
    qt_10_3 """select * from t1 where t1.c1 in (select avg(c1) from t1 group by 2 having avg(c1) > 2) and t1.c2 in (select avg(c1) from t1 group by 2 having avg(c1) > 2) limit 0;"""

}
