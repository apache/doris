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

suite("eliminate_not_null2") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"

    sql """DROP TABLE IF EXISTS t1"""
    sql """DROP TABLE IF EXISTS t2"""
    sql """DROP TABLE IF EXISTS t3"""
    sql """DROP TABLE IF EXISTS t4"""

    sql """
    CREATE TABLE IF NOT EXISTS t1(
      `c1` int(32) NULL,
      `c2` int(32) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 3
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t2(
      `c1` int(32) NULL,
      `c2` int(32) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 3
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t3(
      `c1` int(32) NOT NULL,
      `c2` int(32) NOT NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 3
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t4(
      `c1` int(32) NOT NULL,
      `c2` int(32) NOT NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(c1) BUCKETS 3
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    // single table nullable
    qt_single_nullable1 """explain shape plan select count(1) from t1 where t1.c1 > 0;"""

    qt_single_nullable2 """explain shape plan select count(1) from t1 where t1.c1 is not null;"""

    qt_single_nullable3 """explain shape plan select count(1) from t1 where t1.c1 > 0 and t1.c1 is not null;"""

    qt_single_nullable4 """explain shape plan select count(1) from t1 where t1.c1 > 0 and t1.c2 is not null;"""

    // single table not nullable
    qt_single_notnullable1 """explain shape plan select count(1) from t3 where t3.c1 > 0;"""

    qt_single_notnullable2 """explain shape plan select count(1) from t3 where t3.c1 is not null;"""

    qt_single_notnullable3 """explain shape plan select count(1) from t3 where t3.c1 > 0 and t3.c1 is not null;"""

    qt_single_notnullable4 """explain shape plan select count(1) from t3 where t3.c1 > 0 and t3.c2 is not null;"""

    // nullable inner join
    qt_join_nullable1 """explain shape plan select count(1) from t1, t2 where t1.c1 = t2.c1;
    """

    qt_join_nullable2 """explain shape plan select count(1) from t1, t2 where t1.c1 = t2.c1 and t1.c1 > 0;
    """

    qt_join_nullable3 """explain shape plan select count(1) from t1, t2
    where t1.c1 = t2.c1 and t1.c1 > 0 and t1.c1 is not null;
    """

    qt_join_nullable4 """explain shape plan select count(1) from t1, t2
    where t1.c1 = t2.c1 and t1.c1 > 0 and t1.c2 is not null;
    """

    qt_join_nullable5 """explain shape plan select count(1) from t1, t2
    where t1.c1 = t2.c1 and t1.c2 > 0;
    """

    qt_join_nullable6 """explain shape plan select count(1) from t1, t2
    where t1.c1 = t2.c1 and t1.c2 > 0 and t1.c2 is not null;
    """

    qt_join_nullable7 """explain shape plan select count(1) from t1, t2
    where t1.c1 = t2.c1 and t1.c2 > 0 and t1.c1 is not null;
    """

    // not nullable inner join
    qt_join_notnullable1 """explain shape plan select count(1) from t3, t4 where t3.c1 = t4.c1;
    """

    qt_join_notnullable2 """explain shape plan select count(1) from t3, t4 where t3.c1 = t4.c1 and t3.c1 > 0;
    """

    qt_join_notnullable3 """explain shape plan select count(1) from t3, t4
    where t3.c1 = t4.c1 and t3.c1 > 0 and t3.c1 is not null;
    """

    qt_join_notnullable4 """explain shape plan select count(1) from t3, t4
    where t3.c1 = t4.c1 and t3.c1 > 0 and t3.c2 is not null;
    """

    qt_join_notnullable5 """explain shape plan select count(1) from t3, t4
    where t3.c1 = t4.c1 and t3.c2 > 0;
    """

    qt_join_notnullable6 """explain shape plan select count(1) from t3, t4
    where t3.c1 = t4.c1 and t3.c2 > 0 and t3.c2 is not null;
    """

    qt_join_notnullable7 """explain shape plan select count(1) from t3, t4
    where t3.c1 = t4.c1 and t3.c2 > 0 and t3.c1 is not null;
    """
}
