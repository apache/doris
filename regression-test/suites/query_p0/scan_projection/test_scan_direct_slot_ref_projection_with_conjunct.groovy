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

suite("test_scan_direct_slot_ref_projection_with_conjunct", "query,p0") {
    sql "drop table if exists test_scan_direct_slot_ref_projection_with_conjunct"
    sql """
        create table test_scan_direct_slot_ref_projection_with_conjunct (
            k int not null,
            v_int int null,
            v_big bigint null,
            v_nullable int null,
            v_str varchar(32) null,
            v_double double null
        ) engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        insert into test_scan_direct_slot_ref_projection_with_conjunct values
            (1, 10, 100, null, 'alpha', 1.5),
            (2, 20, 200, 20, 'beta', 2.5),
            (3, 30, 300, null, 'gamma', 3.5),
            (4, 40, 400, 40, 'delta', 4.5),
            (5, 50, 500, 50, 'epsilon', 5.5),
            (6, 60, 600, null, 'zeta', 6.5)
    """

    order_qt_common_partial_filter """
        select v_int
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k in (2, 4, 5)
        order by v_int
    """

    order_qt_all_pass_filter """
        select v_big
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k >= 1
        order by v_big
    """

    order_qt_all_filtered """
        select v_int
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k < 0
        order by v_int
    """

    order_qt_nullable_projected_column """
        select v_nullable
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k in (1, 2, 3, 4)
        order by k
    """

    order_qt_nullable_predicate """
        select k, v_nullable
        from test_scan_direct_slot_ref_projection_with_conjunct
        where v_nullable is null or v_int = 40
        order by k
    """

    order_qt_string_predicate """
        select k, v_str
        from test_scan_direct_slot_ref_projection_with_conjunct
        where v_str like '%ta' or v_str = 'alpha'
        order by k
    """

    order_qt_compound_predicate """
        select k, v_int, v_str
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k between 2 and 5 and (v_nullable is not null or v_str = 'gamma')
        order by k
    """

    order_qt_reordered_and_duplicated_slot_refs """
        select v_str, k, v_str
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k in (2, 3, 5)
        order by k
    """

    order_qt_multi_type_slot_refs """
        select k, v_big, v_double, v_str
        from test_scan_direct_slot_ref_projection_with_conjunct
        where v_double >= 2.5 and v_double <= 5.5
        order by k
    """

    order_qt_non_slot_ref_projection_fallback """
        select v_int + 1, upper(v_str)
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k in (2, 5)
        order by 1
    """

    order_qt_limit_fallback """
        select v_int
        from test_scan_direct_slot_ref_projection_with_conjunct
        where k > 1
        order by k
        limit 3
    """
}
