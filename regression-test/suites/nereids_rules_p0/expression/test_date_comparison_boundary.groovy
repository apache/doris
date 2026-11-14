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

suite("test_date_comparison_boundary") {
    sql "DROP TABLE IF EXISTS test_date_comparison_boundary"
    // One row is sufficient. All source values permit each tested date operation.
    // The error comes from arithmetic on the comparison boundary, not from this row.
    sql """CREATE TABLE test_date_comparison_boundary (
        id INT, d DATE, dt DATETIME(6), dv2 DATETIMEV2(0),
        tz TIMESTAMPTZ(6), ns TIMESTAMP_NS, upper_bound DATETIMEV2(0)
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO test_date_comparison_boundary VALUES (
        1, '2026-11-13', '2026-11-13 15:03:12.123456', '2026-11-13 15:03:12',
        '2026-11-13 15:03:12.123456+00:00', '2026-11-13 15:03:12.123456789',
        '9999-12-31 23:59:59')"""

    // This is the reported query. Moving subtraction to the right adds one day to year 9999.
    order_qt_reported """SELECT dv2 FROM test_date_comparison_boundary
        WHERE days_sub(dv2, 1) < '9999-12-31 23:59:59'"""

    // Use each type's exact limits. Explicit UTC offsets make the TZ limits independent of the session zone.
    def bounds = [
        d: ["0000-01-01", "9999-12-31"],
        dt: ["0000-01-01 00:00:00", "9999-12-31 23:59:59.999999"],
        dv2: ["0000-01-01 00:00:00", "9999-12-31 23:59:59"],
        tz: ["0000-01-01 00:00:00+00:00", "9999-12-31 23:59:59.999999+00:00"],
        ns: ["1677-09-21 00:12:43.145224192", "2262-04-11 23:47:16.854775807"]
    ]
    bounds.each { column, limits ->
        // The same stored row supplies a NULL input. No second row is needed.
        // A normal comparison must return NULL. Null-safe equality must return false.
        "order_qt_${column}_null"("""SELECT
            days_sub(if(id = 1, NULL, ${column}), 1) < '${limits[1]}',
            days_sub(if(id = 1, NULL, ${column}), 1) <=> '${limits[1]}'
            FROM test_date_comparison_boundary""")
        ["weeks", "days", "hours", "minutes", "seconds"].each { unit ->
            // Operations on hours, minutes, and seconds return DATETIME for DATE. Use its upper limit.
            def upperBoundary = column == "d" && !(unit in ["weeks", "days"])
                    ? "9999-12-31 23:59:59" : limits[1]
            // Check both limits and both interval signs. All four predicates must return the one row.
            "order_qt_${column}_${unit}_limits"("""SELECT id FROM test_date_comparison_boundary
                WHERE ${unit}_sub(${column}, 1) < '${upperBoundary}'
                  AND ${unit}_add(${column}, 1) > '${limits[0]}'
                  AND ${unit}_add(${column}, -1) < '${upperBoundary}'
                  AND ${unit}_sub(${column}, -1) > '${limits[0]}'""")
            // The results must be true, true, false, false, false, false.
            // An invalid inverse must not replace these results with NULL.
            def comparisons = ["<", "<=", ">", ">=", "=", "<=>"].collect { op ->
                "${unit}_sub(${column}, 1) ${op} '${upperBoundary}'"
            }.join(", ")
            "order_qt_${column}_${unit}_comparisons"(
                    "SELECT ${comparisons} FROM test_date_comparison_boundary")
        }
    }

    // A column can supply the upper limit. It cannot be checked by constant folding during planning.
    order_qt_column_boundary """SELECT id FROM test_date_comparison_boundary
        WHERE days_sub(dv2, 1) < upper_bound"""

    // SQL aliases and INTERVAL expressions bind to the same day arithmetic functions.
    order_qt_aliases """SELECT id FROM test_date_comparison_boundary
        WHERE date_sub(dv2, 1) < '9999-12-31 23:59:59'
          AND date_add(dv2, -1) < '9999-12-31 23:59:59'
          AND subdate(dv2, 1) < '9999-12-31 23:59:59'
          AND adddate(dv2, -1) < '9999-12-31 23:59:59'
          AND dv2 - INTERVAL 1 DAY < '9999-12-31 23:59:59'
          AND dv2 + INTERVAL 1 DAY > '0000-01-01 00:00:00'
          AND timestampadd(DAY, -1, dv2) < '9999-12-31 23:59:59'
          AND '9999-12-31 23:59:59' > days_sub(dv2, 1)"""

    // Month and year arithmetic cannot be reversed because month lengths differ.
    // Smaller time units and compound intervals do not use the inverse comparison rule.
    order_qt_other_units """SELECT id FROM test_date_comparison_boundary
        WHERE years_sub(dv2, 1) < '9999-12-31 23:59:59'
          AND quarters_sub(dv2, 1) < '9999-12-31 23:59:59'
          AND months_sub(dv2, 1) < '9999-12-31 23:59:59'
          AND milliseconds_sub(dt, 1) < '9999-12-31 23:59:59.999999'
          AND microseconds_sub(tz, 1) < '9999-12-31 23:59:59.999999+00:00'
          AND nanoseconds_sub(ns, 1) < '2262-04-11 23:47:16.854775807'
          AND dv2 - INTERVAL '1 00:00:00' DAY_SECOND < '9999-12-31 23:59:59'"""

    // Each step must be checked. The outer inverse is valid; the inner inverse exceeds the range.
    order_qt_nested """SELECT id FROM test_date_comparison_boundary
        WHERE days_sub(days_sub(dv2, 1), 1) < '9999-12-30 23:59:59'"""

    // Valid inverse boundaries must still allow the existing optimization.
    explain {
        sql "SELECT id FROM test_date_comparison_boundary WHERE days_sub(dv2, 1) < '2026-11-14 15:03:12'"
        notContains "days_sub"
        contains "2026-11-15 15:03:12"
    }
    // Checking an optional rewrite must not send an invalid inverse to BE constant folding.
    def originalFoldByBe = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    try {
        sql "SET enable_fold_constant_by_be = true"
        order_qt_be_folding """SELECT id FROM test_date_comparison_boundary
            WHERE days_sub(dv2, 1) < '9999-12-31 23:59:59'"""
    } finally {
        sql "SET enable_fold_constant_by_be = ${originalFoldByBe}"
    }
    // The fix must not hide an error in date arithmetic that the user actually requested.
    test {
        sql """SELECT id FROM test_date_comparison_boundary
            WHERE days_add(upper_bound, 1) > '0000-01-01 00:00:00'"""
        exception "out of range"
    }
}
