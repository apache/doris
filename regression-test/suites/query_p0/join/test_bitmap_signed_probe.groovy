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

suite("test_bitmap_signed_probe", "p0") {
    // Reuse one two-row table for probes and bitmap input. The union contains only 1.
    sql "drop table if exists test_bitmap_signed_probe"
    sql """create table test_bitmap_signed_probe (k bigint not null, b bitmap bitmap_union)
        aggregate key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1")"""
    sql "insert into test_bitmap_signed_probe values (-1, to_bitmap(1)), (1, to_bitmap(1))"

    // A negative SQL integer cannot equal an unsigned bitmap element. IN excludes -1;
    // NOT IN keeps it. Neither predicate is an invalid direct bitmap_contains call.
    order_qt_in """select k from test_bitmap_signed_probe
        where k in (select b from test_bitmap_signed_probe)"""
    order_qt_not_in """select k from test_bitmap_signed_probe
        where k not in (select b from test_bitmap_signed_probe)"""
    // Constants must also pass planning without the direct-call negative-element check.
    order_qt_literal """select k from test_bitmap_signed_probe
        where -1 not in (select b from test_bitmap_signed_probe)"""
    // NULL probes must keep the existing NULL result and must not pass NOT IN.
    order_qt_null_probe """select k from test_bitmap_signed_probe
        where nullif(k, -1) not in (select b from test_bitmap_signed_probe)"""
    // Exercise the mark-join path and a computed probe, not only a slot in a filter.
    order_qt_mark """select k from test_bitmap_signed_probe
        where k not in (select b from test_bitmap_signed_probe) or k = 1"""
    // A NULL probe must also remain NULL inside a mark join; only k = 1 passes the OR.
    order_qt_null_mark """select k from test_bitmap_signed_probe
        where nullif(k, -1) not in (select b from test_bitmap_signed_probe) or k = 1"""
    order_qt_expression """select k from test_bitmap_signed_probe
        where k - 1 not in (select b from test_bitmap_signed_probe)"""
    // An empty union does not contain any non-NULL probe, including negative values.
    order_qt_empty """select k from test_bitmap_signed_probe
        where k not in (select b from test_bitmap_signed_probe where false)"""
    // Clamping a probe must not make -1 match 0 or UINT64_MAX.
    order_qt_unsigned_boundary """select k from test_bitmap_signed_probe
        where k in (select bitmap_from_string('0,18446744073709551615'))"""
}
