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

suite("inpredicate") {
    sql """
        SET enable_nereids_planner=true
    """

    sql "SET enable_fallback_to_original_planner=false"

    order_qt_in_predicate_1 """
        SELECT * FROM supplier WHERE s_suppkey in (1, 2, 3);
    """

    order_qt_in_predicate_2 """
        SELECT * FROM supplier WHERE s_suppkey not in (1, 2, 3);
    """

    order_qt_in_predicate_3 """
        SELECT * FROM supplier WHERE s_suppkey in (1, 2, 128, 129);
    """

    order_qt_in_predicate_4 """
        SELECT * FROM supplier WHERE s_suppkey in (1, 2, 128, 32768, 32769);
    """

    order_qt_in_predicate_5 """
        SELECT * FROM supplier WHERE s_suppkey in (1, 2, 128, 32768, 2147483648);
    """

    order_qt_in_predicate_6 """
        SELECT * FROM supplier WHERE s_suppkey not in (1, 2, 128, 32768, 2147483648);
    """

    order_qt_in_predicate_7 """
        SELECT * FROM supplier WHERE s_nation in ('PERU', 'ETHIOPIA');
    """

    order_qt_in_predicate_8 """
        SELECT * FROM supplier WHERE s_nation not in ('PERU', 'ETHIOPIA');
    """

    order_qt_in_predicate_9 """
        SELECT * FROM supplier WHERE s_suppkey in (15);
    """

    order_qt_in_predicate_10 """
        SELECT * FROM supplier WHERE s_suppkey not in (15);
    """

    order_qt_in_predicate_11 """
        SELECT * FROM supplier WHERE s_suppkey in (15, null);
    """

    order_qt_in_predicate_12 """
        SELECT * FROM supplier WHERE s_suppkey not in (15, null);
    """

    order_qt_in_predicate_13 """
        SELECT * FROM supplier WHERE s_nation in ('PERU', 'ETHIOPIA', null);
    """

    order_qt_in_predicate_14 """
        SELECT * FROM supplier WHERE s_nation not in ('PERU', 'ETHIOPIA', null);
    """
}

