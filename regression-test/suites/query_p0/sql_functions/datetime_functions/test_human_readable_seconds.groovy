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

// human_readable_seconds follows Presto/Trino semantics:
//   - rounds to whole seconds with round(abs(x))   (no fractional / millisecond output)
//   - the sign is dropped (absolute value)
//   - emits only weeks / days / hours / minutes / seconds
//   - NaN / Infinity are rejected
// This suite checks three things:
//   1. vectorized BE execution on nullable / not-nullable / const columns (qt_ + .out),
//   2. FE constant folding produces exactly the same string as BE execution,
//   3. NaN / Infinity raise an error on both the FE-fold and BE-exec paths.
suite("test_human_readable_seconds") {
    // ====================================================================
    // Part 1: vectorized BE execution + nullable / const column handling
    // ====================================================================
    sql "drop table if exists test_human_readable_seconds"
    sql """
        create table test_human_readable_seconds (
            k0 int,
            a double not null,
            b double null
        )
        distributed by hash(k0)
        properties
        (
            "replication_num" = "1"
        );
    """

    qt_empty_nullable "select human_readable_seconds(b) from test_human_readable_seconds order by k0"
    qt_empty_not_nullable "select human_readable_seconds(a) from test_human_readable_seconds order by k0"

    sql "insert into test_human_readable_seconds values (1, 1, null), (2, 1, null), (3, 1, null)"
    qt_all_null "select human_readable_seconds(b) from test_human_readable_seconds order by k0"

    sql "truncate table test_human_readable_seconds"
    sql """
        insert into test_human_readable_seconds values
        (1, 96, 96),
        (2, 3762, 3762),
        (3, 56363463, 56363463),
        (4, 0, 0),
        (5, -96, -96),
        (6, 0.9, 0.9),
        (7, 1.2, 1.2),
        (8, 61, 61),
        (9, 604800, 604800),
        (10, 3600, null);
    """

    qt_nullable "select human_readable_seconds(b) from test_human_readable_seconds order by k0"
    qt_not_nullable "select human_readable_seconds(a) from test_human_readable_seconds order by k0"
    qt_nullable_no_null "select human_readable_seconds(nullable(a)) from test_human_readable_seconds order by k0"
    qt_const_nullable "select human_readable_seconds(NULL) from test_human_readable_seconds order by k0"
    qt_const_not_nullable "select human_readable_seconds(3762) from test_human_readable_seconds order by k0"
    qt_const_nullable_no_null "select human_readable_seconds(nullable(3762))"

    // ====================================================================
    // Part 2: golden correctness via qt_ on a DOUBLE column (vectorized BE).
    //
    // Each value flows through a column, so it is evaluated by the BE (a column
    // is never constant-folded); expected output is recorded in the .out file.
    // The list mixes integer and fractional literals on purpose: integers are
    // coerced to DOUBLE by the signature. 1e20 / -1e20 are passed as strings so
    // they reach SQL as 1e20, not Groovy's BigDecimal rendering "1E+20".
    // ====================================================================
    def goldenValues = [
            // zero / sub-second rounding
            0, 0.4, 0.5, 0.9, 1, 1.5,
            // minute boundary (round-half-up lands exactly on 60)
            59, 59.4, 59.5, 60, 60.4, 60.5, 61, 119, 120,
            // hour boundary
            3599, 3599.5, 3600, 3601, 3660, 7199, 7200,
            // day boundary
            86399, 86400, 90061, 172800,
            // week boundary
            604799, 604800, 691200, 1209600,
            // Presto/Trino documentation examples
            96, 3762, 56363463,
            // negatives use absolute value (no leading '-')
            -0.5, -59.5, -96, -3762,
            // large values + out-of-range finite saturation at int64_t max
            1000000, 535333.9513888889, 535333.2513888889, 1234567890, "1e20", "-1e20"
    ]

    sql "drop table if exists hrs_golden"
    sql """
        create table hrs_golden (id int, val double not null)
        distributed by hash(id) properties ("replication_num" = "1");
    """
    def goldenRows = []
    goldenValues.eachWithIndex { v, i -> goldenRows.add("(${i + 1}, ${v})") }
    sql "insert into hrs_golden values ${goldenRows.join(", ")}"

    qt_golden "select human_readable_seconds(val) from hrs_golden order by id"

    // ====================================================================
    // Part 3: FE constant folding must match BE execution for every golden
    // value above plus a broad sweep around each unit boundary. testFoldConst
    // runs the same query with folding on and off and asserts identical rows
    // (it also disables enable_sql_cache internally), which is exactly the
    // FE/BE consistency check and is cheap to extend to many more values.
    // ====================================================================
    def consistencyValues = new ArrayList(goldenValues)
    for (int v = 0; v <= 130; v += 5) { consistencyValues.add(v) }          // around minute
    for (int v = 3590; v <= 3610; v += 2) { consistencyValues.add(v) }      // around hour
    for (int v = 86390; v <= 86410; v += 2) { consistencyValues.add(v) }    // around day
    for (int v = 604790; v <= 604810; v += 2) { consistencyValues.add(v) }  // around week
    [7199.5, -1, -60, -3600, -86400, 123456789, 535334].each { consistencyValues.add(it) }

    consistencyValues.each { v ->
        testFoldConst("select human_readable_seconds(${v})")
    }

    // ====================================================================
    // Part 4: NaN / Infinity are rejected on both the BE-exec and FE-fold paths.
    // Both implementations raise an "Invalid argument value ..." error.
    // ====================================================================
    ['true', 'false'].each { skip ->
        sql "set debug_skip_fold_constant=${skip}"
        test {
            sql "select human_readable_seconds(cast('nan' as double))"
            exception "Invalid argument value"
        }
        test {
            sql "select human_readable_seconds(cast('inf' as double))"
            exception "Invalid argument value"
        }
        test {
            sql "select human_readable_seconds(-cast('inf' as double))"
            exception "Invalid argument value"
        }
    }
    sql "set debug_skip_fold_constant=false"
}
