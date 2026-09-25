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

suite("test_percentile_reservoir_constant_level") {
    sql "SET enable_agg_state = true"

    // a literal level and an equivalent foldable constant expression must behave the same
    qt_literal_level """
        SELECT percentile_reservoir(number, 0.5) FROM numbers('number' = '10')
    """
    qt_foldable_level """
        SELECT percentile_reservoir(number, 0.25 + 0.25) FROM numbers('number' = '10')
    """
    qt_foldable_cast_level """
        SELECT percentile_reservoir(number, cast('0.5' as double)) FROM numbers('number' = '10')
    """
    qt_foldable_level_group_by """
        SELECT number % 2 AS k, percentile_reservoir(number, 1 - 0.75)
        FROM numbers('number' = '10') GROUP BY k ORDER BY k
    """
    qt_foldable_level_window """
        SELECT number, percentile_reservoir(number, 0.5 * 2) OVER (PARTITION BY number % 2)
        FROM numbers('number' = '6') ORDER BY number
    """
    qt_string_literal_level """
        SELECT percentile_reservoir(number, '0.5') FROM numbers('number' = '10')
    """
    qt_foldable_level_state """
        SELECT percentile_reservoir_merge(s) FROM (
            SELECT percentile_reservoir_state(cast(number as double), 0.25 + 0.25) s
            FROM numbers('number' = '10')
        ) states
    """

    // INSERT ... VALUES is planned without the rewrite phase, the level is still validated there
    sql "DROP TABLE IF EXISTS test_percentile_reservoir_constant_level_state"
    sql """
        CREATE TABLE test_percentile_reservoir_constant_level_state (
            k INT NOT NULL,
            s AGG_STATE<percentile_reservoir(DOUBLE NOT NULL, DOUBLE NOT NULL)> GENERIC
        ) AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_percentile_reservoir_constant_level_state
        VALUES (1, percentile_reservoir_state(cast(7 as double), 0.25 + 0.25))
    """
    qt_values_foldable_level_state """
        SELECT k, percentile_reservoir_merge(s)
        FROM test_percentile_reservoir_constant_level_state GROUP BY k ORDER BY k
    """
    test {
        sql """
            INSERT INTO test_percentile_reservoir_constant_level_state
            VALUES (2, percentile_reservoir_state(cast(7 as double), 2.0))
        """
        exception "percentile_reservoir level must be in [0, 1]"
    }

    // a foldable constant outside [0, 1] is still rejected
    test {
        sql "SELECT percentile_reservoir(number, 0.5 + 1) FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1]"
    }
    test {
        sql "SELECT percentile_reservoir(number, -0.5 - 0.5) FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1]"
    }
    test {
        sql """
            SELECT percentile_reservoir_state(cast(number as double), 0.5 + 1)
            FROM numbers('number' = '10')
        """
        exception "percentile_reservoir level must be in [0, 1]"
    }

    test {
        sql "SELECT percentile_reservoir(number, '5') FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1]"
    }
    test {
        sql """
            INSERT INTO test_percentile_reservoir_constant_level_state
            VALUES (3, percentile_reservoir_state(cast(7 as double), 0.5 + 1))
        """
        exception "percentile_reservoir level must be in [0, 1]"
    }

    // a string level takes the same implicit cast as signature coercion, so '' and
    // cast('' as double) agree: NULL under the default non-strict cast, an error under strict cast
    qt_empty_string_level """
        SELECT percentile_reservoir(number, '') FROM numbers('number' = '10')
    """
    qt_empty_string_cast_level """
        SELECT percentile_reservoir(number, cast('' as double)) FROM numbers('number' = '10')
    """
    sql "SET enable_strict_cast = true"
    test {
        sql "SELECT percentile_reservoir(number, '') FROM numbers('number' = '10')"
        exception "can't cast to double in strict mode"
    }
    test {
        sql "SELECT percentile_reservoir(number, cast('' as double)) FROM numbers('number' = '10')"
        exception "can't cast to double in strict mode"
    }
    sql "SET enable_strict_cast = false"

    // FE parses a NaN payload like BE, so such a string level is rejected as NaN instead of
    // being accepted as a failed (NULL) cast that BE would still execute as NaN
    qt_nan_payload_cast """
        SELECT cast('nan(foo)' as double), cast(' -nan(ind) ' as double), cast('nan(a-b)' as double)
    """
    for (String level : ["'nan(foo)'", "cast('nan(foo)' as double)"]) {
        test {
            sql "SELECT percentile_reservoir(number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
        test {
            sql "SELECT percentile_reservoir(DISTINCT number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
        test {
            sql "SELECT percentile_reservoir_state(number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
    }

    // FE casts a string to FLOAT/DOUBLE with the exact bits BE produces: a leading '-' gives a
    // negative NaN, and FLOAT is parsed as a double first and then narrowed with a single rounding,
    // so a value just above the midpoint between 1 and the next float ties to 1. A level derived
    // from those bits is the same whether FE folds it or BE executes it, see the skip-fold block below
    qt_signed_nan_and_float_midpoint """
        SELECT signbit(cast('-nan(foo)' as double)), signbit(cast(' -nan ' as double)),
               signbit(cast('nan(foo)' as double)), signbit(cast(cast('-nan(foo)' as float) as double)),
               cast('1.00000005960464483090177623170427978038787841796875' as float) > cast(1 as float)
    """
    for (String level : ["cast(signbit(cast('-nan(foo)' as double)) as double)",
            "cast((cast('1.00000005960464483090177623170427978038787841796875' as float) > cast(1 as float)) as double)",
            "cast((cast(cast('1.0000000596046448' as double) as float) > cast(1 as float)) as double)"]) {
        qt_bit_derived_level """
            SELECT percentile_reservoir(number, ${level}), percentile_reservoir(DISTINCT number, ${level})
            FROM numbers('number' = '10')
        """
    }
    sql """
        INSERT INTO test_percentile_reservoir_constant_level_state
        VALUES (4, percentile_reservoir_state(cast(7 as double), cast(signbit(cast('-nan(foo)' as double)) as double)))
    """

    // DECIMALV2 division folds on FE the way BE executes it: NULL only for a zero divisor
    qt_decimalv2_divide """
        SELECT cast(0 as decimalv2(27, 9)) / cast(2 as decimalv2(27, 9)),
               cast(1 as decimalv2(27, 9)) / cast(0 as decimalv2(27, 9))
    """
    qt_decimalv2_zero_dividend_level """
        SELECT percentile_reservoir(number, cast(0 as decimalv2(27, 9)) / cast(2 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_decimalv2_zero_divisor_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(0 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """

    // a nonzero DECIMAL quotient folds on FE with the scale and rounding BE uses (DECIMALV2 keeps
    // scale 9 and rounds, DECIMALV3 truncates at the result scale), so it is a valid level
    qt_decimal_quotient """
        SELECT cast(1 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)),
               cast(1 as decimalv2(27, 9)) / cast(1024 as decimalv2(27, 9)),
               cast(-2 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)),
               1.0 / 3, 2.0 / 3, -2.0 / 3, 1.0 / 1024
    """
    qt_decimalv2_quotient_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_decimalv2_small_quotient_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(1024 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_decimalv3_quotient_level """
        SELECT percentile_reservoir(number, 2.0 / 3) FROM numbers('number' = '10')
    """
    test {
        sql "SELECT percentile_reservoir(number, 4.0 / 3) FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1], but got 1.33333"
    }

    // a constant that FE cannot fold is rejected instead of being clamped by BE
    test {
        sql "SELECT percentile_reservoir(number, pow(0.5, 1)) FROM numbers('number' = '10')"
        exception "percentile_reservoir"
    }

    // a non-constant level is still rejected
    test {
        sql "SELECT percentile_reservoir(number, number / 10) FROM numbers('number' = '10')"
        exception "percentile_reservoir requires second parameter must be a constant"
    }

    // the level is validated even when constant folding is left to BE
    sql "SET debug_skip_fold_constant = true"
    qt_skip_fold_literal_level """
        SELECT percentile_reservoir(number, 0.5) FROM numbers('number' = '10')
    """
    qt_skip_fold_foldable_level """
        SELECT percentile_reservoir(number, 0.25 + 0.25) FROM numbers('number' = '10')
    """
    test {
        sql "SELECT percentile_reservoir(number, 0.5 + 1) FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1]"
    }
    qt_skip_fold_empty_string_level """
        SELECT percentile_reservoir(number, '') FROM numbers('number' = '10')
    """
    qt_skip_fold_empty_string_cast_level """
        SELECT percentile_reservoir(number, cast('' as double)) FROM numbers('number' = '10')
    """
    sql "SET enable_strict_cast = true"
    test {
        sql "SELECT percentile_reservoir(number, '') FROM numbers('number' = '10')"
        exception "can't cast to double in strict mode"
    }
    sql "SET enable_strict_cast = false"
    qt_skip_fold_nan_payload_cast """
        SELECT cast('nan(foo)' as double), cast(' -nan(ind) ' as double), cast('nan(a-b)' as double)
    """
    for (String level : ["'nan(foo)'", "cast('nan(foo)' as double)"]) {
        test {
            sql "SELECT percentile_reservoir(number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
        test {
            sql "SELECT percentile_reservoir(DISTINCT number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
        test {
            sql "SELECT percentile_reservoir_state(number, ${level}) FROM numbers('number' = '10')"
            exception "percentile_reservoir level must be in [0, 1], but got NaN"
        }
    }
    qt_skip_fold_decimalv2_divide """
        SELECT cast(0 as decimalv2(27, 9)) / cast(2 as decimalv2(27, 9)),
               cast(1 as decimalv2(27, 9)) / cast(0 as decimalv2(27, 9))
    """
    qt_skip_fold_decimalv2_zero_dividend_level """
        SELECT percentile_reservoir(number, cast(0 as decimalv2(27, 9)) / cast(2 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_skip_fold_decimalv2_zero_divisor_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(0 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    // BE computes these quotients now, they must match the FE-folded values above
    qt_skip_fold_decimal_quotient """
        SELECT cast(1 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)),
               cast(1 as decimalv2(27, 9)) / cast(1024 as decimalv2(27, 9)),
               cast(-2 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)),
               1.0 / 3, 2.0 / 3, -2.0 / 3, 1.0 / 1024
    """
    qt_skip_fold_decimalv2_quotient_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(3 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_skip_fold_decimalv2_small_quotient_level """
        SELECT percentile_reservoir(number, cast(1 as decimalv2(27, 9)) / cast(1024 as decimalv2(27, 9)))
        FROM numbers('number' = '10')
    """
    qt_skip_fold_decimalv3_quotient_level """
        SELECT percentile_reservoir(number, 2.0 / 3) FROM numbers('number' = '10')
    """
    test {
        sql "SELECT percentile_reservoir(number, 4.0 / 3) FROM numbers('number' = '10')"
        exception "percentile_reservoir level must be in [0, 1], but got 1.33333"
    }
    qt_skip_fold_signed_nan_and_float_midpoint """
        SELECT signbit(cast('-nan(foo)' as double)), signbit(cast(' -nan ' as double)),
               signbit(cast('nan(foo)' as double)), signbit(cast(cast('-nan(foo)' as float) as double)),
               cast('1.00000005960464483090177623170427978038787841796875' as float) > cast(1 as float)
    """
    for (String level : ["cast(signbit(cast('-nan(foo)' as double)) as double)",
            "cast((cast('1.00000005960464483090177623170427978038787841796875' as float) > cast(1 as float)) as double)",
            "cast((cast(cast('1.0000000596046448' as double) as float) > cast(1 as float)) as double)"]) {
        qt_skip_fold_bit_derived_level """
            SELECT percentile_reservoir(number, ${level}), percentile_reservoir(DISTINCT number, ${level})
            FROM numbers('number' = '10')
        """
    }
    // a state whose level BE computes merges with the stored state whose level FE folded above;
    // INSERT ... VALUES always folds on FE, so the BE-computed state is built inline, and coalesce
    // keeps its level NOT NULL like the stored AGG_STATE type
    qt_skip_fold_bit_derived_level_state_merge """
        SELECT percentile_reservoir_merge(s) FROM (
            SELECT s FROM test_percentile_reservoir_constant_level_state WHERE k = 4
            UNION ALL
            SELECT percentile_reservoir_state(cast(9 as double),
                    coalesce(cast(signbit(cast('-nan(foo)' as double)) as double), 0))
            FROM numbers('number' = '1')
        ) states
    """
    sql "SET debug_skip_fold_constant = false"
}
