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

suite("subquery_basic_pullup_uk") {
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
    UNIQUE KEY (c1)
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
    UNIQUE KEY (c1)
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
    UNIQUE KEY (c1)
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
     * 6. null/not null/uk/non-uk
     */
    // 1. single table + olap table + filter + uk + correlated + equal
    qt_1_1 """select * from t1 where t1.c1 in (select c1 from t2);"""
    qt_1_2 """select * from t1 where t1.c1 not in (select c1 from t2);"""

    qt_1_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2);"""
    qt_1_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2);"""

    qt_1_5 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 + 1);"""
    qt_1_6 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 + 1);"""

    qt_1_7 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1);"""
    qt_1_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1);"""

    qt_1_9 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 + 1);"""
    qt_1_10 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 + 1);"""

    // 2. single table + olap table + filter + uk + correlated + non-eqau
    qt_2_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 < t2.c2);"""
    qt_2_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 < t2.c2);"""

    qt_2_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 < t2.c2 + 1);"""
    qt_2_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 < t2.c2 + 1);"""

    qt_2_5 """select * from t1 where exists (select c1 from t2 where t1.c1 < t2.c1);"""
    qt_2_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 < t2.c1);"""

    qt_2_7 """select * from t1 where exists (select c1 from t2 where t1.c1 < t2.c1 + 1);"""
    qt_2_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 < t2.c1 + 1);"""

    // 3. single table + olap table + filter + uk + correlated + non-equal
    qt_3_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 <= t2.c2);"""
    qt_3_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 <= t2.c2);"""

    qt_3_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 <= t2.c2 + 1);"""
    qt_3_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 <= t2.c2 + 1);"""

    qt_3_5 """select * from t1 where exists (select c1 from t2 where t1.c1 <= t2.c1);"""
    qt_3_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 <= t2.c1);"""

    qt_3_7 """select * from t1 where exists (select c1 from t2 where t1.c1 <= t2.c1 + 1);"""
    qt_3_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 <= t2.c1 + 1);"""

    // 4. single table + olap table + filter + uk + correlated + non-equal
    qt_4_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 != t2.c2);"""
    qt_4_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 != t2.c2);"""

    qt_4_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 != t2.c2 + 1);"""
    qt_4_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 != t2.c2 + 1);"""

    qt_4_5 """select * from t1 where exists (select c1 from t2 where t1.c1 != t2.c1);"""
    qt_4_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 != t2.c1);"""

    qt_4_7 """select * from t1 where exists (select c1 from t2 where t1.c1 != t2.c1 + 1);"""
    qt_4_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 != t2.c1 + 1);"""

    // 5. single table + olap table + filter + uk + correlated + non-equal
    qt_5_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 <=> t2.c2);"""
    qt_5_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 <=> t2.c2);"""

    qt_5_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 <=> t2.c2 + 1);"""
    qt_5_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 <=> t2.c2 + 1);"""

    qt_5_5 """select * from t1 where exists (select c1 from t2 where t1.c1 <=> t2.c1);"""
    qt_5_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 <=> t2.c1);"""

    qt_5_7 """select * from t1 where exists (select c1 from t2 where t1.c1 <=> t2.c1 + 1);"""
    qt_5_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 <=> t2.c1 + 1);"""

    // 6. single table + olap table + filter + uk + correlated + (mixed equal & non-equal)
    qt_6_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3);"""
    qt_6_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 and t1.c3 < t2.c3);"""

    qt_6_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 + 1 and t1.c3 < t2.c3);"""
    qt_6_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 + 1 and t1.c3 < t2.c3);"""

    qt_6_5 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 and t1.c3 < t2.c3);"""
    qt_6_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 and t1.c3 < t2.c3);"""

    qt_6_7 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 + 1 and t1.c3 < t2.c3);"""
    qt_6_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 + 1 and t1.c3 < t2.c3);"""

    // 7. single table + olap table + filter + uk + non-correlated
    qt_7_1 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = 1);"""
    qt_7_2 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = 1);"""

    qt_7_3 """select * from t1 where exists (select c1 from t2 where t1.c1 = 1);"""
    qt_7_4 """select * from t1 where not exists (select c1 from t2 where t1.c1 = 1);"""

    // 8. single table + olap table + filter + uk + scalar
    qt_8_1 """select * from t1 where t1.c1 in (select max(c1) from t2);"""
    qt_8_2 """select * from t1 where t1.c1 not in (select max(c1) from t2);"""

    qt_8_3 """select * from t1 where t1.c1 in (select max(c1) from t2 where t1.c2 = t2.c2);"""
    qt_8_4 """select * from t1 where t1.c1 not in (select max(c1) from t2 where t1.c2 = t2.c2);"""

    qt_8_5 """select * from t1 where exists (select max(c1) from t2 where t1.c1 = t2.c1);"""
    qt_8_6 """select * from t1 where not exists (select max(c1) from t2 where t1.c1 = t2.c1);"""

    qt_8_7 """select * from t1 where exists (select max(c1), count(c2) from t2 where t1.c1 = t2.c1);"""
    qt_8_8 """select * from t1 where not exists (select max(c1), count(c2) from t2 where t1.c1 = t2.c1);"""

    // 9. single table + olap table + filter + uk + correlated + limit
    qt_9_1 """select * from t1 where t1.c1 in (select c1 from t2 limit 1);"""
    qt_9_2 """select * from t1 where t1.c1 not in (select c1 from t2 limit 1);"""

    qt_9_3 """select * from t1 where t1.c1 in (select c1 from t2 limit 0);"""
    qt_9_4 """select * from t1 where t1.c1 not in (select c1 from t2 limit 0);"""

    qt_9_5 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 limit 1);"""
    qt_9_6 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 limit 1);"""

    qt_9_7 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 limit 0);"""
    qt_9_8 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 limit 0);"""

    qt_9_9 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 limit 1);"""
    qt_9_10 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 limit 1);"""

    qt_9_11 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 limit 0);"""
    qt_9_12 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 limit 0);"""

    // 10. single table + olap table + filter + uk + group by / having
    qt_10_1 """select * from t1 where t1.c1 in (select c1 from t2 group by c1 having c1 > 1);"""
    qt_10_2 """select * from t1 where t1.c1 not in (select c1 from t2 group by c1 having c1 > 1);"""

    qt_10_3 """select * from t1 where t1.c1 in (select c1 from t2 where t1.c2 = t2.c2 group by c1 having c1 > 1);"""
    qt_10_4 """select * from t1 where t1.c1 not in (select c1 from t2 where t1.c2 = t2.c2 group by c1 having c1 > 1);"""

    qt_10_5 """select * from t1 where exists (select c1 from t2 where t1.c1 = t2.c1 group by c1 having c1 > 1);"""
    qt_10_6 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 group by c1 having c1 > 1);"""

    qt_10_7 """select * from t1 where exists (select c1, max(c2) from t2 where t1.c1 = t2.c1 group by c1 having max(c2) > 1);"""
    qt_10_8 """select * from t1 where not exists (select c1 from t2 where t1.c1 = t2.c1 group by c1 having max(c2) > 1);"""
}

