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
    sql "SET debug_skip_fold_constant = false"
}
