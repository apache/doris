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

suite("join") {
    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    order_qt_inner_join_1 """
        SELECT * FROM lineorder JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_inner_join_2 """
        SELECT * FROM lineorder INNER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_outer_join_1 """
        SELECT * FROM lineorder LEFT JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_outer_join_2 """
        SELECT * FROM lineorder LEFT OUTER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_outer_join_1 """
        SELECT * FROM lineorder RIGHT JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_outer_join_2 """
        SELECT * FROM lineorder RIGHT OUTER JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_semi_join """
        SELECT * FROM lineorder LEFT SEMI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_semi_join """
        SELECT * FROM lineorder RIGHT SEMI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_left_anti_join """
        SELECT * FROM lineorder LEFT ANTI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_right_anti_join """
        SELECT * FROM lineorder RIGHT ANTI JOIN supplier ON lineorder.lo_suppkey = supplier.s_suppkey
    """

    order_qt_cross_join """
        SELECT * FROM lineorder CROSS JOIN supplier;
    """

    order_qt_inner_join_with_other_condition """
        select lo_orderkey, lo_partkey, p_partkey, p_size from lineorder inner join part on lo_partkey = p_partkey where lo_orderkey - 1310000 > p_size;
    """

    order_qt_outer_join_with_filter """
        select lo_orderkey, lo_partkey, p_partkey, p_size from lineorder inner join part on lo_partkey = p_partkey where lo_orderkey - 1310000 > p_size;
    """
}

