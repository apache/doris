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

// Regression coverage for a Variant V2 storage bug: within a single segment, when a path's first
// value is a JSON bool and a plain number arrives afterwards, get_numeric_type() (BE
// core/data_type/get_least_supertype.cpp) counts TYPE_BOOLEAN as an 8-bit unsigned integer, so the
// path's least-common-type computation folded BOOL and the numeric type into a single numeric
// storage type instead of falling back to JSONB. promote() then cast the already-written `true`
// value to that numeric type, turning it into `1` in storage -- a real Variant/JSON semantics
// violation (true must never compare or group equal to 1). The opposite order (a number first, then
// a bool) already fell back to JSONB and stayed correct; the "rev" path below is that same-order
// control, which must keep passing unchanged.
//
// Note on the queries below: Doris rejects any direct comparison predicate on a raw
// VARIANT/JSON-typed expression ("... does not support ordering/comparison, CAST to a concrete type
// first"), and CAST(... AS STRING)/CAST(... AS BOOLEAN) both render a SQL boolean as "1"/"0" (normal,
// unrelated SQL cast semantics), which would mask this exact bug instead of exposing it. So the
// checks below use the value's own canonical JSON text (the plain, uncast SELECT output) and GROUP
// BY on the raw variant sub-path (which only needs hashing/equality, not ordering) to observe the
// real stored value instead.
suite("test_variant_bool_numeric_widening", "p0") {
    def variantV2Function = "parse_to_variant"
    def table_name = "test_variant_bool_numeric_widening"

    sql "drop table if exists ${table_name}"

    sql """
        create table ${table_name} (
            id int,
            var variant<properties("variant_max_subcolumns_count" = "0")>
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """

    // A single INSERT so every row lands in the same segment and the path builder sees the values
    // in exactly this order.
    // "fwd": bool first, then numbers -- the buggy order.
    // "rev": a number first, then a bool -- the pre-existing, already-correct control order.
    sql """
        insert into ${table_name} (id, var) values
            (1, ${variantV2Function}('{"fwd": true}')),
            (2, ${variantV2Function}('{"fwd": 1}')),
            (3, ${variantV2Function}('{"fwd": 0}')),
            (4, ${variantV2Function}('{"fwd": 1.5}')),
            (5, ${variantV2Function}('{"rev": 1}')),
            (6, ${variantV2Function}('{"rev": 0}')),
            (7, ${variantV2Function}('{"rev": 1.5}')),
            (8, ${variantV2Function}('{"rev": true}'))
    """

    // The raw values must keep their JSON kind: id=1's `true` must print as `true`, not `1`.
    qt_sql_fwd_values """
        select id, var['fwd']
        from ${table_name}
        where var['fwd'] is not null
        order by id
    """
    qt_sql_rev_values """
        select id, var['rev']
        from ${table_name}
        where var['rev'] is not null
        order by id
    """

    // GROUP BY must not collapse the bool row (id=1, `true`) with the numeric row that has the
    // "same" 0/1 value (id=2, `1`): if `true` were stored as `1`, both would land in one group of
    // size 2 instead of two groups of size 1. `min(id)` stands in for the (unorderable) group key so
    // the result is deterministic.
    qt_sql_fwd_group """
        select count(*) as cnt, min(id) as first_id
        from ${table_name}
        where var['fwd'] is not null
        group by var['fwd']
        order by first_id
    """
    qt_sql_rev_group """
        select count(*) as cnt, min(id) as first_id
        from ${table_name}
        where var['rev'] is not null
        group by var['rev']
        order by first_id
    """

    // Comparing a raw Variant path directly is rejected by design (independent of this bug); confirm
    // that restriction still applies the same way for both the buggy and the control order.
    test {
        sql """ select id from ${table_name} where var['fwd'] = ${variantV2Function}('1') """
        exception "CAST to a concrete type first"
    }
    test {
        sql """ select id from ${table_name} where var['rev'] = ${variantV2Function}('1') """
        exception "CAST to a concrete type first"
    }
}
