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

suite("sumRewrite") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
        DROP TABLE IF EXISTS sr
       """
    sql """
    CREATE TABLE IF NOT EXISTS sr(
      `id` int NULL,
      `null_id` int not NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    INSERT INTO sr (id, null_id) VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (4, 2),
    (5, 3),
    (null, 3),
    (null, 4),
    (8, 4),
    (9, 5),
    (10, 5);
    """

    order_qt_sum_add_const$ """ select sum(id + 2) from sr """

    order_qt_sum_add_const_alias$ """ select sum(id + 2) as result from sr """

    order_qt_sum_add_const_where$ """ select sum(id + 2) from sr where id is not null """

    order_qt_sum_add_const_group_by$ """ select null_id, sum(id + 2) from sr group by null_id """

    order_qt_sum_add_const_having$ """ select null_id, sum(id + 2) from sr group by null_id having sum(id + 2) > 5 """

    order_qt_sum_sub_const$ """ select sum(id - 2) from sr """

    order_qt_sum_sub_const_alias$ """ select sum(id - 2) as result from sr """

    order_qt_sum_sub_const_where$ """ select sum(id - 2) from sr where id is not null """

    order_qt_sum_sub_const_group_by$ """ select null_id, sum(id - 2) from sr group by null_id """

    order_qt_sum_sub_const_having$ """ select null_id, sum(id - 2) from sr group by null_id having sum(id - 2) > 0 """

    order_qt_sum_add_const_empty_table$ """ select sum(id + 2) from sr where 1=0 """

    order_qt_sum_add_const_empty_table_group_by$ """ select null_id, sum(id + 2) from sr where 1=0 group by null_id """

    order_qt_sum_sub_const_empty_table$ """ select sum(id - 2) from sr where 1=0 """

    order_qt_sum_sub_const_empty_table_group_by$ """ select null_id, sum(id - 2) from sr where 1=0 group by null_id """
}