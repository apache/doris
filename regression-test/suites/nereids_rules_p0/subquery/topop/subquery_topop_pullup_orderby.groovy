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

suite("subquery_basic_pullup_orderby") {
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
    // 1. single table + olap table + order by + correlated + equal
    qt_1_1 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t2);"""
    qt_1_2 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t2);"""

    qt_1_3 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t2);"""
    qt_1_4 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t2);"""

    qt_1_5 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t2);"""
    qt_1_6 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t2);"""

    qt_1_7 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t2);"""
    qt_1_8 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t2);"""

    qt_1_9 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t3);"""
    qt_1_10 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t3);"""

    qt_1_11 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t3);"""
    qt_1_12 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t3);"""

    qt_1_13 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t3);"""
    qt_1_14 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t3);"""

    qt_1_15 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t3);"""
    qt_1_16 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t3);"""

    qt_1_17 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t2);"""
    qt_1_18 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t2);"""

    qt_1_19 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t2);"""
    qt_1_20 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t2);"""

    qt_1_21 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3);"""
    qt_1_22 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3);"""

    qt_1_23 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3);"""
    qt_1_24 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3);"""

    qt_1_25 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3);"""
    qt_1_26 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3);"""

    qt_1_27 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3);"""
    qt_1_28 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3);"""

    // 2. single table + olap table + order by + correlated + explicit equal
    qt_2_1 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_2 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_3 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_4 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_5 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_6 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_7 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_8 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_9 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_10 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_11 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_12 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_13 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_14 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_15 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_16 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_17 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_18 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_19 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t2 where t1.c4 = t2.c4);"""
    qt_2_20 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t2 where t1.c4 = t2.c4);"""

    qt_2_21 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_22 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_23 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_24 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_25 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_26 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3 where t1.c4 = t3.c4);"""

    qt_2_27 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3 where t1.c4 = t3.c4);"""
    qt_2_28 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3 where t1.c4 = t3.c4);"""

    // 3. single table + olap table + order by + correlated + explicit non-equal
    qt_3_1 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_2 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_3 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_4 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_5 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_6 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_7 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_8 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_9 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_10 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_11 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_12 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_13 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_14 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_15 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_16 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_17 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_18 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_19 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t2 where t1.c4 < t2.c4);"""
    qt_3_20 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t2 where t1.c4 < t2.c4);"""

    qt_3_21 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_22 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_23 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_24 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_25 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_26 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select c2 from t3 where t1.c4 < t3.c4);"""

    qt_3_27 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c1 from t3 where t1.c4 < t3.c4);"""
    qt_3_28 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select c2 from t3 where t1.c4 < t3.c4);"""

    // 4. single table + olap table + order by + correlated + scalar order by
    qt_4_1 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_2 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_3 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_4 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_5 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_6 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_7 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_8 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_9 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_10 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 in (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_11 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_12 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by t1.c1 not in (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_13 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_14 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 in (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_15 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_16 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by t1.c2 not in (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_17 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_18 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_19 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select max(c1) from t2 where t1.c4 = t2.c4);"""
    qt_4_20 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select count(c2) from t2 where t1.c4 = t2.c4);"""

    qt_4_21 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_22 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by exists (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_23 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_24 """select t1.c1 from t1 where t1.c1 in (select c1 from t2) order by not exists (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_25 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_26 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by exists (select count(c2) from t3 where t1.c4 = t3.c4);"""

    qt_4_27 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select max(c1) from t3 where t1.c4 = t3.c4);"""
    qt_4_28 """select t1.c2 from t1 where t1.c1 in (select c1 from t2) order by not exists (select count(c2) from t3 where t1.c4 = t3.c4);"""
}

