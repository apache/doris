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

// A cast of a VARBINARY value to a string type keeps its bytes, so hex(), to_base64() and length() of a
// VARBINARY constant, which take the value through that cast, must give the same results whether FE folds
// the constant (fold), FE and BE fold it (fold_by_be) or BE evaluates it (no_fold); the results of a block
// must match in the three modes. X'FF' is not valid UTF-8, so FE leaves its cast to BE. So does text with a
// NUL byte or a supplementary character, which FE does not evaluate like BE (edge): 'a', NUL, 'b' is not equal
// to 'a', and VARCHAR(1) and trim_in keep U+1F600 (F09F9880) whole. Such a cast also stays in a scan filter,
// whose conjuncts the condition cache digests (filter), and in the mark join of a nullable IN subquery (in).
// Each mode puts its name in a column alias, so that it is not served the cached result of another mode.

suite("test_varbinary_cast_fold") {
    sql "drop table if exists test_varbinary_cast_fold_t"
    sql """
        CREATE TABLE test_varbinary_cast_fold_t (k int, s string NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')
    """
    // BE evaluates the casts, so the rows hold the bytes: 'a', 'a' NUL 'b', 0xFF, U+FFFD, NULL and U+1F600.
    sql "set enable_fold_constant_by_be = false"
    sql "set debug_skip_fold_constant = true"
    sql """
        INSERT INTO test_varbinary_cast_fold_t VALUES (1, 'a'), (2, cast(X'610062' AS string)),
            (3, cast(X'FF' AS string)), (4, cast(X'EFBFBD' AS string)), (5, NULL), (6, cast(X'F09F9880' AS string))
    """
    order_qt_data "SELECT k, hex(s), length(s) FROM test_varbinary_cast_fold_t"

    sql "set enable_condition_cache = true"
    def modes = [
            fold      : [false, false],
            fold_by_be: [true, false],
            no_fold   : [false, true]]
    modes.each { mode, settings ->
        sql "set enable_fold_constant_by_be = ${settings[0]}"
        sql "set debug_skip_fold_constant = ${settings[1]}"
        "order_qt_hex_${mode}" """
            SELECT hex(cast('abc' AS varbinary(16))) AS hex_${mode}, hex(X'E4B8AD'), hex(X'FF'), hex(X'')
        """
        "order_qt_base64_${mode}" """
            SELECT to_base64(cast('abc' AS varbinary(16))) AS base64_${mode}, to_base64(X'FF')
        """
        "order_qt_cast_${mode}" """
            SELECT cast(cast('abc' AS varbinary(16)) AS string) AS cast_${mode},
                   length(cast(cast('abc' AS varbinary(16)) AS varchar(16))),
                   cast(X'E4B8AD' AS string),
                   length(cast(X'FF' AS string))
        """
        "order_qt_edge_${mode}" """
            SELECT cast(X'610062' AS string) = 'a' AS edge_${mode},
                   hex(cast(X'F09F9880' AS varchar(1))),
                   hex(trim_in(cast(X'F09F9880' AS string), cast(X'F09F9881' AS string)))
        """
        "order_qt_filter_${mode}" """
            SELECT 'ff' AS filter_${mode}, k FROM test_varbinary_cast_fold_t WHERE s = cast(X'FF' AS string)
            UNION ALL
            SELECT 'nul', k FROM test_varbinary_cast_fold_t WHERE s = cast(X'610062' AS string)
            UNION ALL
            SELECT 'emoji', k FROM test_varbinary_cast_fold_t WHERE s = cast(X'F09F9880' AS string)
            UNION ALL
            SELECT 'emoji_varchar1', k FROM test_varbinary_cast_fold_t
            WHERE s = cast(cast(X'F09F9880' AS string) AS varchar(1))
            UNION ALL
            SELECT 'array', k FROM test_varbinary_cast_fold_t WHERE array_contains(cast([X'FF'] AS array<string>), s)
            UNION ALL
            SELECT 'utf8', k FROM test_varbinary_cast_fold_t WHERE s = cast(X'61' AS string)
        """
        "order_qt_in_${mode}" """
            SELECT cast(X'610062' AS string) IN (SELECT s FROM test_varbinary_cast_fold_t) AS in_${mode},
                   cast(X'FF' AS string) IN (SELECT s FROM test_varbinary_cast_fold_t WHERE k <> 4),
                   cast(X'F09F9880' AS string) NOT IN (SELECT s FROM test_varbinary_cast_fold_t WHERE s IS NOT NULL),
                   cast(X'616263' AS string) IN (SELECT s FROM test_varbinary_cast_fold_t WHERE s IS NOT NULL)
        """
    }
}
