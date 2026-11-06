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

// CASE WHEN with a VARBINARY result: with two or more WHEN branches it is not rewritten to if() and BE
// selects the value by the result type. The branches mix a column cast, binary literals with NUL and
// non-UTF-8 bytes and an expression; without ELSE a row that no branch selects is NULL. The results must
// match whether FE folds constants (fold), FE and BE fold them (fold_by_be) or BE evaluates them (no_fold).
// Each mode puts its name in a column alias, so that it is not served the cached result of another mode.

suite("test_varbinary_case_when") {
    sql "drop table if exists test_varbinary_case_when_t"
    sql """
        CREATE TABLE test_varbinary_case_when_t (k int, s string NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_varbinary_case_when_t VALUES
            (1, 'a'), (2, 'xyz'), (3, ''), (4, NULL), (5, 'bc'), (6, 'def'), (7, 'g'), (8, 'hi'), (9, 'j')
    """

    def modes = [
            fold      : [false, false],
            fold_by_be: [true, false],
            no_fold   : [false, true]]
    modes.each { mode, settings ->
        sql "set enable_fold_constant_by_be = ${settings[0]}"
        sql "set debug_skip_fold_constant = ${settings[1]}"
        "order_qt_with_else_${mode}" """
            SELECT k, hex(CASE WHEN k % 3 = 0 THEN cast(s AS varbinary)
                               WHEN k % 3 = 1 THEN X'00FF'
                               ELSE cast(concat('c', k) AS varbinary) END) AS with_else_${mode}
            FROM test_varbinary_case_when_t
        """
        "order_qt_without_else_${mode}" """
            SELECT k, hex(CASE WHEN k % 3 = 0 THEN cast(s AS varbinary)
                               WHEN k % 3 = 1 THEN X'' END) AS without_else_${mode}
            FROM test_varbinary_case_when_t
        """
        "order_qt_numbers_${mode}" """
            SELECT number, hex(CASE WHEN number % 3 = 0 THEN cast(concat('a', number) AS varbinary)
                                    WHEN number % 3 = 1 THEN cast('y' AS varbinary)
                                    ELSE X'FF' END) AS numbers_${mode}
            FROM numbers("number" = "6")
        """
        "order_qt_constant_${mode}" """
            SELECT hex(CASE WHEN 1 = 2 THEN X'01' WHEN 2 = 2 THEN X'FF00' ELSE X'02' END) AS constant_${mode},
                   hex(CASE WHEN 1 = 2 THEN X'01' WHEN 2 = 3 THEN X'02' END)
        """
    }
}
