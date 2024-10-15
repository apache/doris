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

suite("push_down_top_n_distinct_through_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql 'set be_number_for_test=3'
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"

    sql """
        DROP TABLE IF EXISTS table_join;
    """

    sql """
    CREATE TABLE IF NOT EXISTS table_join(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
        insert into table_join values
            (0, NULL, 'Test'),
            (1, 1, 'Test'),
            (2, 2, 'Test'),
            (3, 3, 'Test'),
            (4, 4, 'Test'),
            (5, 5, 'Test'),
            (6, 6, 'Test'),
            (7, 7, 'Test');
    """

    qt_push_down_topn_through_join """
        explain shape plan select distinct * from (select t1.id from table_join t1 left join table_join t2 on t1.id = t2.id) t order by id limit 10;
    """

    qt_push_down_topn_through_join_data """
        select distinct * from (select t1.id from table_join t1 left join table_join t2 on t1.id = t2.id) t order by id limit 10;
    """

    qt_push_down_topn_through_join """
        explain shape plan select distinct * from (select t1.id from table_join t1 cross join table_join t2) t order by id limit 10;
    """

    qt_push_down_topn_through_join_data """
        select distinct * from (select t1.id from table_join t1 cross join table_join t2) t order by id limit 10;
    """
}