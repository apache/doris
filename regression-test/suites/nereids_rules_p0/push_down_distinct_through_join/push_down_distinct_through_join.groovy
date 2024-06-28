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

suite("push_down_distinct_through_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"

    sql """
        DROP TABLE IF EXISTS t;
    """

    sql """
    CREATE TABLE IF NOT EXISTS t(
      `id` int(32),
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "insert into t values (1, 1, 'a')"
    sql "insert into t values (2, null, 'a')"
    sql "insert into t values (3, 1, null)"
    sql "insert into t values (4, 2, 'b')"
    sql "insert into t values (5, null, 'b')"
    sql "insert into t values (6, 2, null)"
    sql "insert into t values (7, 3, 'c')"
    sql "insert into t values (8, null, 'c')"
    sql "insert into t values (9, 3, null)"
    sql "insert into t values (10, null, null)"

    qt_basic_not """
        explain shape plan select /*+ USE_CBO_RULE(push_down_distinct_through_join) */ distinct * from (select t1.id from t t1 join t t2 on t1.id = t2.id) t;
    """

    qt_basic """
        explain shape plan select /*+ USE_CBO_RULE(push_down_distinct_through_join) */ distinct * from (select t1.id from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id) t;
    """

    order_qt_basic_sql """
        select /*+ USE_CBO_RULE(push_down_distinct_through_join) */ distinct * from (select t1.id from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id) t;
    """

    order_qt_basic_sql_disable """
        select /*+ USE_CBO_RULE(no_push_down_distinct_through_join) */ distinct * from (select t1.id from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id) t;
    """
}
