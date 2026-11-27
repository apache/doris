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

suite("test_round_numeric_boundaries", "p0") {
    // FE must reject the BE-invalid scale before NULL propagation removes the function.
    // These checks need no table or input rows.
    def roundingFunctions = ["round", "round_bankers", "truncate", "floor", "ceil", "dround", "dfloor", "dceil"]
    roundingFunctions.each { function ->
        test {
            sql "select ${function}(null, -32769)"
            exception "Scale argument for function"
        }
        test {
            sql "select ${function}(cast(1 as double), 32768)"
            exception "Scale argument for function"
        }
        test {
            sql "select ${function}(cast(1 as double), -32769)"
            exception "Scale argument for function"
        }
    }

    // One stored row supplies the source cases' scale and DOUBLE boundary values.
    sql "drop table if exists test_round_numeric_boundaries"
    sql """create table test_round_numeric_boundaries (
        k int, i int, n bigint, x double
    ) duplicate key(k) distributed by hash(k) buckets 1
    properties("replication_num" = "1")"""
    sql "insert into test_round_numeric_boundaries values (0, -2147483648, -1, 1e-310)"

    // A column keeps rounding on BE. A literal exercises FE with the same value.
    // The decimal factor for scale -20 must be 10^20, not UINT64_MAX.
    order_qt_round_column """select round(cast('1e19' as double) + n, -20),
        round_bankers(cast('1e19' as double) + n, -20),
        truncate(cast('1e19' as double) + n, -20),
        floor(cast('1e19' as double) + n, -20),
        ceil(cast('1e19' as double) + n, -20)
        from test_round_numeric_boundaries"""
    order_qt_round_literal "select round(cast('1e19' as double), -20)"
    // A decimal unit retains precision when its reciprocal overflows DOUBLE.
    order_qt_positive_rounding """select round(x, 309) = 0, round_bankers(x, 309) = 0,
        truncate(x, 309) = 0, floor(x, 309) = 0,
        ceil(x, 309) = cast('1e-309' as double),
        floor(-x, 309) = cast('-1e-309' as double), ceil(-x, 309) = 0,
        round(x, 32767) = x from test_round_numeric_boundaries"""
    // These aliases must reach the same BE rounding implementation with a column argument.
    order_qt_rounding_aliases """select dround(cast('1e19' as double) + k, -20),
        dfloor(cast('1e19' as double) + k, -20), dceil(cast('1e19' as double) + k, -20)
        from test_round_numeric_boundaries"""
    test {
        sql "select floor(cast(-1 as double) + k, -309) from test_round_numeric_boundaries"
        exception "Arithmetic overflow"
    }
    // FE and BE use the same signed 16-bit scale. Constant folding must not hide an invalid scale.
    roundingFunctions.each { function ->
        test {
            sql "select ${function}(cast(1 as double), i) from test_round_numeric_boundaries"
            exception "Scale argument for function"
        }
    }
}
