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
      `not_null_id` int not NULL,
      `f_id` float NULL,
      `d_id` decimal(10,2),
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
INSERT INTO sr (id, not_null_id, f_id, d_id) VALUES 
(11, 6, 1.1, 210.01),
(12, 6, 2.2, 220.02),
(13, 7, 3.3, 230.03),
(14, 7, 4.4, 240.04),
(15, 8, 5.5, 250.05),
(null, 8, 6.6, 260.06),
(null, 9, 7.7, 270.07),
(18, 9, 8.8, 280.08),
(19, 10, 9.9, 290.09),
(20, 10, 10.1, 400.10);
"""

    order_qt_sum_add_const$ """ select sum(id + 2) from sr """

    order_qt_sum_add_const_alias$ """ select sum(id + 2) as result from sr """

    order_qt_sum_add_const_where$ """ select sum(id + 2) from sr where id is not null """

    order_qt_sum_add_const_group_by$ """ select not_null_id, sum(id + 2) from sr group by not_null_id """

    order_qt_sum_add_const_having$ """ select not_null_id, sum(id + 2) from sr group by not_null_id having sum(id + 2) > 5 """

    order_qt_sum_sub_const$ """ select sum(id - 2) from sr """

    order_qt_sum_sub_const_alias$ """ select sum(id - 2) as result from sr """

    order_qt_sum_sub_const_where$ """ select sum(id - 2) from sr where id is not null """

    order_qt_sum_sub_const_group_by$ """ select not_null_id, sum(id - 2) from sr group by not_null_id """

    order_qt_sum_sub_const_having$ """ select not_null_id, sum(id - 2) from sr group by not_null_id having sum(id - 2) > 0 """

    order_qt_sum_add_const_empty_table$ """ select sum(id + 2) from sr where 1=0 """

    order_qt_sum_add_const_empty_table_group_by$ """ select not_null_id, sum(id + 2) from sr where 1=0 group by not_null_id """

    order_qt_sum_sub_const_empty_table$ """ select sum(id - 2) from sr where 1=0 """

    order_qt_sum_sub_const_empty_table_group_by$ """ select not_null_id, sum(id - 2) from sr where 1=0 group by not_null_id """

    // float类型字段测试
    order_qt_float_sum_add_const$ """ select sum(f_id + 2) from sr """

    order_qt_float_sum_add_const_alias$ """ select sum(f_id + 2) as result from sr """

    order_qt_float_sum_add_const_where$ """ select sum(f_id + 2) from sr where f_id is not null """

    order_qt_float_sum_add_const_group_by$ """ select not_null_id, sum(f_id + 2) from sr group by not_null_id """

    order_qt_float_sum_add_const_having$ """ select not_null_id, sum(f_id + 2) from sr group by not_null_id having sum(f_id + 2) > 5 """

    order_qt_float_sum_sub_const$ """ select sum(f_id - 2) from sr """

    order_qt_float_sum_sub_const_alias$ """ select sum(f_id - 2) as result from sr """

    order_qt_float_sum_sub_const_where$ """ select sum(f_id - 2) from sr where f_id is not null """

    order_qt_float_sum_sub_const_group_by$ """ select not_null_id, sum(f_id - 2) from sr group by not_null_id """

    order_qt_float_sum_sub_const_having$ """ select not_null_id, sum(f_id - 2) from sr group by not_null_id having sum(f_id - 2) > 0 """

    order_qt_sum_null_and_not_null """ select sum(id), sum(id + 1), sum(id - 1), sum(not_null_id), sum(not_null_id + 1), sum(not_null_id - 1) from sr"""

    order_qt_sum_distinct """select sum(id), sum(distinct id), sum(id + 1), sum(distinct id + 1) from sr"""

    order_qt_sum_not_null_distinct """select sum(not_null_id), sum(distinct not_null_id), sum(not_null_id + 1), sum(distinct not_null_id + 1) from sr"""        
    // 测试精度变化对sum加常数的影响
    // order_qt_decimal_sum_add_const_precision_1$ """ select sum(d_id + 2) from sr """

    // order_qt_decimal_sum_add_const_precision_2$ """ select sum(d_id + 2.2) from sr """

    // order_qt_decimal_sum_add_const_precision_3$ """ select not_null_id, sum(d_id + 2) from sr group by not_null_id """

    // order_qt_decimal_sum_add_const_precision_4$ """ select not_null_id, sum(d_id + 2.223) from sr group by not_null_id """

    // 测试精度变化对sum减常数的影响  
    // order_qt_decimal_sum_sub_const_precision_1$ """ select sum(d_id - 2) from sr """

    // order_qt_decimal_sum_sub_const_precision_2$ """ select sum(d_id - 2.2) from sr """

    // order_qt_decimal_sum_sub_const_precision_3$ """ select not_null_id, sum(d_id - 2) from sr group by not_null_id """

    // order_qt_decimal_sum_sub_const_precision_4$ """ select not_null_id, sum(d_id - 2.223) from sr group by not_null_id """
}