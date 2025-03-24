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

suite("subquery_basic_pullup_winfunc") {
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
    // 1. single table + olap table + partition by + correlated + equal
    qt_1_1 """select c1, row_number() over(partition by c1 in (select c1 from t2)) as rn from t1;"""
    qt_1_2 """select c1, row_number() over(partition by c1 not in (select c1 from t2)) as rn from t1;"""

    qt_1_3 """select c1, row_number() over(partition by exists (select c1 from t2)) as rn from t1;"""
    qt_1_4 """select c1, row_number() over(partition by not exists (select c1 from t2)) as rn from t1;"""

    // 2. single table + olap table + order by + correlated + equal
    qt_2_1 """select c1, row_number() over(order by c1 in (select c1 from t2)) as rn from t1;"""
    qt_2_2 """select c1, row_number() over(order by c1 not in (select c1 from t2)) as rn from t1;"""

    qt_2_3 """select c1, row_number() over(order by exists (select c1 from t2)) as rn from t1;"""
    qt_2_4 """select c1, row_number() over(order by not exists (select c1 from t2)) as rn from t1;"""

    // 3. single table + olap table + partition by + order by + correlated + equal
    qt_3_1 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 in (select c1 from t2)) as rn from t1;"""
    qt_3_2 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1;"""

    qt_3_3 """select c1, row_number() over(partition by exists (select c1 from t2) order by exists (select c1 from t2)) as rn from t1;"""
    qt_3_4 """select c1, row_number() over(partition by not exists (select c1 from t2) order by not exists (select c1 from t2)) as rn from t1;"""

    qt_3_5 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1;"""
    qt_3_6 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 in (select c1 from t2)) as rn from t1;"""

    qt_3_7 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by exists (select c1 from t2)) as rn from t1;"""
    qt_3_8 """select c1, row_number() over(partition by not exists (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1;"""

    qt_3_9 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 in (select c1 from t3)) as rn from t1;"""
    qt_3_10 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 not in (select c1 from t3)) as rn from t1;"""

    qt_3_11 """select c1, row_number() over(partition by exists (select c1 from t2) order by exists (select c1 from t3)) as rn from t1;"""
    qt_3_12 """select c1, row_number() over(partition by not exists (select c1 from t2) order by not exists (select c1 from t3)) as rn from t1;"""

    qt_3_13 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 not in (select c1 from t3)) as rn from t1;"""
    qt_3_14 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 in (select c1 from t3)) as rn from t1;"""

    qt_3_15 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by exists (select c1 from t3)) as rn from t1;"""
    qt_3_16 """select c1, row_number() over(partition by not exists (select c1 from t2) order by c1 not in (select c1 from t3)) as rn from t1;"""

    // 4. single table + olap table + partition by + order by + case when + equal
    qt_4_1 """select c1, row_number() over(partition by case when c1 in (select c1 from t2) then c1 else c2 end order by c1 in (select c1 from t3)) as rn from t1;"""
    qt_4_2 """select c1, row_number() over(partition by case when not c1 in (select c1 from t2) then c1 else c2 end order by c1 not in (select c1 from t3)) as rn from t1;"""

    qt_4_3 """select c1, row_number() over(partition by case when exists (select c1 from t2) then c1 else c2 end order by c1 in (select c1 from t3)) as rn from t1;"""
    qt_4_4 """select c1, row_number() over(partition by case when not exists (select c1 from t2) then c1 else c2 end order by c1 not in (select c1 from t3)) as rn from t1;"""

    // 5. single table + olap table + partition by + order by + nested case when + equal
    qt_5_1 """select c1, row_number() over(partition by case when c1 in (select c1 from t2) then c1 in (select c1 from t3) else c2 end) as rn from t1;"""
    qt_5_2 """select c1, row_number() over(partition by case when not c1 in (select c1 from t2) then c1 not in (select c1 from t3) else c2 end) as rn from t1;"""

    qt_5_3 """select c1, row_number() over(partition by case when exists (select c1 from t2) then c1 in (select c1 from t3) else c2 end) as rn from t1;"""
    qt_5_4 """select c1, row_number() over(partition by case when not exists (select c1 from t2) then c1 not in (select c1 from t3) else c2 end) as rn from t1;"""

    qt_5_5 """select c1, row_number() over(partition by case when c1 in (select c1 from t2) then c1 else c1 in (select c1 from t3) end) as rn from t1;"""
    qt_5_6 """select c1, row_number() over(partition by case when not c1 in (select c1 from t2) then c1 else c1 not in (select c1 from t3) end) as rn from t1;"""

    qt_5_7 """select c1, row_number() over(partition by case when exists (select c1 from t2) then c1 else c1 in (select c1 from t3) end) as rn from t1;"""
    qt_5_8 """select c1, row_number() over(partition by case when not exists (select c1 from t2) then c1 else c1 not in (select c1 from t3) end) as rn from t1;"""

    // 6. single table + olap table + partition by + order by + correlated + equal + filter
    qt_6_1 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 in (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_2 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_3 """select c1, row_number() over(partition by exists (select c1 from t2) order by exists (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_4 """select c1, row_number() over(partition by not exists (select c1 from t2) order by not exists (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_5 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_6 """select c1, row_number() over(partition by c1 not in (select c1 from t2) order by c1 in (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_7 """select c1, row_number() over(partition by c1 in (select c1 from t2) order by exists (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_8 """select c1, row_number() over(partition by not exists (select c1 from t2) order by c1 not in (select c1 from t2)) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_9 """select c1, row_number() over(partition by case when c1 in (select c1 from t2) then c1 in (select c1 from t3) else c2 end) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_10 """select c1, row_number() over(partition by case when not c1 in (select c1 from t2) then c1 not in (select c1 from t3) else c2 end) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_11 """select c1, row_number() over(partition by case when exists (select c1 from t2) then c1 in (select c1 from t3) else c2 end) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_12 """select c1, row_number() over(partition by case when not exists (select c1 from t2) then c1 not in (select c1 from t3) else c2 end) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_13 """select c1, row_number() over(partition by case when c1 in (select c1 from t2) then c1 else c1 in (select c1 from t3) end) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_14 """select c1, row_number() over(partition by case when not c1 in (select c1 from t2) then c1 else c1 not in (select c1 from t3) end) as rn from t1 where c3 in (select c3 from t3);"""

    qt_6_15 """select c1, row_number() over(partition by case when exists (select c1 from t2) then c1 else c1 in (select c1 from t3) end) as rn from t1 where c3 in (select c3 from t3);"""
    qt_6_16 """select c1, row_number() over(partition by case when not exists (select c1 from t2) then c1 else c1 not in (select c1 from t3) end) as rn from t1 where c3 in (select c3 from t3);"""
}

