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

suite("subquery_multitable_pullup_and") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"

    sql """DROP TABLE IF EXISTS t1;"""
    sql """DROP TABLE IF EXISTS t2;"""
    sql """DROP TABLE IF EXISTS t3;"""

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

    sql """
    CREATE TABLE IF NOT EXISTS t3(
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

    sql """INSERT INTO t3 VALUES (null,null,null,null);"""
    sql """INSERT INTO t3 VALUES (1,1,1,1);"""
    sql """INSERT INTO t3 VALUES (2,2,2,2);"""
    sql """INSERT INTO t3 VALUES (3,3,3,3);"""

    /**
     * | DML      | insert/update/delete |
     * | select   | SELECT LIST: SubQuery |
     * | From     | (OlapScan/ExternalScan)/view/partition(hash/range)/MV |
     * | Join     | JOIN TYPE: inner/outer(left/right/full)/semi(left/right)/anti(left/right) ON condition: SubQuery |
     * | Filter   | EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery |
     * | Group by | EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery |
     * | Having   | EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery |
     * | Order by | EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery |
     * | Winfunc  | Partition by / Order by EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery |
     * | Limit    | Limit / offset |
     * | Set op   | union(all)/intersect(all)/except(all) |
     * | SubQuery | Correlated subquery / non-correlated subquery / Scalar |
     * EXPRS: OR/AND/not/in/exists/equal/non-equal/composed expr/SubQuery/bitmap type()
     * 1. (not) in/exists SubQuery
     * 2. (not) in/exists SubQuery OR/AND (not) in/exists SubQuery
     * 3. (not) in/exists SubQuery OR/AND (equal/non-equal)
     * 4. Corelated / non-correlated / Scalar
     * 5. composed expr：SubQuery
     * 6. null/not null/uk/no-uk
     */
    // 1. multi table + olap table + filter/on condition + and + correlated + equal
    qt_1_1 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2) and t1.c1 > 1;"""
    qt_1_2 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2) and t1.c1 > 1;"""

    qt_1_3 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2) and t1.c1 > 1;"""
    qt_1_4 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2) and t1.c1 > 1;"""

    qt_1_5 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2) and t1.c1 > 1;"""
    qt_1_6 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2) and t1.c1 > 1;"""

    qt_1_7 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2) and t1.c1 > 1;"""
    qt_1_8 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2) and t1.c1 > 1;"""

    qt_1_9 """select * from t1, t3 where t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""
    qt_1_10 """select * from t1, t3 where t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""

    qt_1_11 """select * from t1 left join t3 on t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""
    qt_1_12 """select * from t1 left join t3 on t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""

    // 2. multi table + olap table + filter/on condition + and + correlated + non-equal
    qt_2_1 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 < t2.c2) and t1.c1 > 1;"""
    qt_2_2 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 < t2.c2) and t1.c1 > 1;"""

    qt_2_3 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 < t2.c2) and t1.c1 > 1;"""
    qt_2_4 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 < t2.c2) and t1.c1 > 1;"""

    qt_2_5 """select * from t1, t3 where t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 < t2.c1) and t1.c1 > 1;"""
    qt_2_6 """select * from t1, t3 where t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 < t2.c1) and t1.c1 > 1;"""

    qt_2_7 """select * from t1 left join t3 on t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 < t2.c1) and t1.c1 > 1;"""
    qt_2_8 """select * from t1 left join t3 on t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 < t2.c1) and t1.c1 > 1;"""

    // 3. multi table + olap table + filter/on condition + and + correlated + (mixed equal & non-equal)
    qt_3_1 """select * from t1, t3 where t1.c4 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""
    qt_3_2 """select * from t1, t3 where t1.c4 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""

    qt_3_3 """select * from t1 left join t3 on t1.c4 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""
    qt_3_4 """select * from t1 left join t3 on t1.c4 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""

    qt_3_5 """select * from t1, t3 where t1.c4 = t3.c3 and exists (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""
    qt_3_6 """select * from t1, t3 where t1.c4 = t3.c3 and not exists (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""

    qt_3_7 """select * from t1 left join t3 on t1.c4 = t3.c3 and exists (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""
    qt_3_8 """select * from t1 left join t3 on t1.c4 = t3.c3 and not exists (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3) and t1.c1 > 1;"""

    // 4. multi table + olap table + filter/on condition + and + non-correlated
    qt_4_1 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""
    qt_4_2 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""

    qt_4_3 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""
    qt_4_4 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""

    qt_4_5 """select * from t1, t3 where t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""
    qt_4_6 """select * from t1, t3 where t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""

    qt_4_7 """select * from t1 left join t3 on t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""
    qt_4_8 """select * from t1 left join t3 on t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c2 = 1) and t1.c1 > 1;"""

    // 5. multi table + olap table + filter/on condition + and + scalar
    qt_5_1 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select max(c1) from t2) and t1.c1 > 1;"""
    qt_5_2 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select max(c1) from t2) and t1.c1 > 1;"""

    qt_5_3 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select max(c1) from t2) and t1.c1 > 1;"""
    qt_5_4 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select max(c1) from t2) and t1.c1 > 1;"""

    qt_5_5 """select * from t1, t3 where t1.c3 = t3.c3 and exists (select max(c1) from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""
    qt_5_6 """select * from t1, t3 where t1.c3 = t3.c3 and not exists (select max(c1) from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""

    qt_5_7 """select * from t1 left join t3 on t1.c3 = t3.c3 and exists (select max(c1) from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""
    qt_5_8 """select * from t1 left join t3 on t1.c3 = t3.c3 and not exists (select max(c1) from t2 where t1.c1 = t2.c1) and t1.c1 > 1;"""

    // 6. multi table + olap table + filter/on condition + and + correlated + limit
    qt_6_1 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 limit 1) and t1.c1 > 1;"""
    qt_6_2 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 limit 0) and t1.c1 > 1;"""

    qt_6_3 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 limit 1) and t1.c1 > 1;"""
    qt_6_4 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 limit 0) and t1.c1 > 1;"""

    qt_6_5 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 limit 1) and t1.c1 > 1;"""
    qt_6_6 """select * from t1, t3 where t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 limit 0) and t1.c1 > 1;"""

    qt_6_7 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 limit 1) and t1.c1 > 1;"""
    qt_6_8 """select * from t1 left join t3 on t1.c3 = t3.c3 and t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 limit 0) and t1.c1 > 1;"""

    qt_6_9 """select * from t1, t3 where t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 = t2.c1 limit 0) and t1.c1 > 1;"""
    qt_6_10 """select * from t1, t3 where t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 = t2.c1 limit 1) and t1.c1 > 1;"""

    qt_6_11 """select * from t1 left join t3 on t1.c3 = t3.c3 and exists (select c1 from t2 where t1.c1 = t2.c1 limit 0) and t1.c1 > 1;"""
    qt_6_12 """select * from t1 left join t3 on t1.c3 = t3.c3 and not exists (select c1 from t2 where t1.c1 = t2.c1 limit 1) and t1.c1 > 1;"""
}

