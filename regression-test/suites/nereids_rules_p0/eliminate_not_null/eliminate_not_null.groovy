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

suite("eliminate_not_null") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    sql """
        DROP TABLE IF EXISTS t
    """
    sql """
    CREATE TABLE IF NOT EXISTS t(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_eliminate_not_null_basic_comparison """
        explain shape plan select * from t where score > 13 and score is not null;
    """

    qt_eliminate_not_null_in_clause """
        explain shape plan select * from t where id in (1, 2, 3) and id is not null;
    """

    qt_eliminate_not_null_not_equal """
        explain shape plan select * from t where score != 13 and score is not null;
    """

    qt_eliminate_not_null_string_function """
        explain shape plan select * from t where LENGTH(name) > 0 and name is not null;
    """

    qt_eliminate_not_null_aggregate """
        explain shape plan select AVG(score) from t where score > 0 and score is not null group by id;
    """

    qt_eliminate_not_null_between """
        explain shape plan select * from t where score between 1 and 10 and score is not null;
    """

    qt_eliminate_not_null_math_function """
        explain shape plan select * from t where ABS(score) = 5 and score is not null;
    """

    qt_eliminate_not_null_complex_logic """
        explain shape plan select * from t where (score > 5 or id < 10) and score is not null;
    """

    qt_eliminate_not_null_date_function """
        explain shape plan select * from t where YEAR(name) = 2022 and name is not null;
    """

    qt_eliminate_not_null_with_subquery """
        explain shape plan select * from t where score in (select score from t where score > 0) and score is not null;
    """
}
