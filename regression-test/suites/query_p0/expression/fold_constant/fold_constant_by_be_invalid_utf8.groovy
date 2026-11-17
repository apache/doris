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

// BE folds a constant string to its bytes, and FE can build a string literal from them only when they are
// valid UTF-8. unhex('FF'), and the strings, arrays, maps and structs built from it, hold the byte 0xFF, which a
// FE literal would turn into U+FFFD (EFBFBD). So they must give the same results whether FE folds the
// constants (fold), FE and BE fold them (fold_by_be) or BE evaluates them (no_fold); the results of a block
// must match in the three modes. BE still folds a string that is valid UTF-8, such as unhex('C3A9') ('é').
// Each mode puts its name in a column alias, so that it is not served the cached result of another mode.

suite("fold_constant_by_be_invalid_utf8") {
    sql "drop table if exists fold_constant_by_be_invalid_utf8_t"
    sql """
        CREATE TABLE fold_constant_by_be_invalid_utf8_t (k int, s string NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')
    """
    // BE evaluates unhex, so the rows hold the bytes: 'a', 0xFF, U+FFFD, 'a' 0xFF, NULL and 'é'.
    sql "set enable_fold_constant_by_be = false"
    sql "set debug_skip_fold_constant = true"
    sql """
        INSERT INTO fold_constant_by_be_invalid_utf8_t VALUES
            (1, 'a'), (2, unhex('FF')), (3, unhex('EFBFBD')), (4, unhex('61FF')), (5, NULL), (6, unhex('C3A9'))
    """
    order_qt_data "SELECT k, hex(s) FROM fold_constant_by_be_invalid_utf8_t"

    def modes = [
            fold      : [false, false],
            fold_by_be: [true, false],
            no_fold   : [false, true]]
    modes.each { mode, settings ->
        sql "set enable_fold_constant_by_be = ${settings[0]}"
        sql "set debug_skip_fold_constant = ${settings[1]}"
        "order_qt_filter_${mode}" """
            SELECT 'ff' AS filter_${mode}, k FROM fold_constant_by_be_invalid_utf8_t WHERE s = unhex('FF')
            UNION ALL
            SELECT 'concat', k FROM fold_constant_by_be_invalid_utf8_t WHERE s = concat('a', unhex('FF'))
            UNION ALL
            SELECT 'array', k FROM fold_constant_by_be_invalid_utf8_t WHERE array_contains(array(unhex('FF')), s)
            UNION ALL
            SELECT 'map', k FROM fold_constant_by_be_invalid_utf8_t WHERE map_contains_key(map(unhex('FF'), 1), s)
            UNION ALL
            SELECT 'struct', k FROM fold_constant_by_be_invalid_utf8_t
            WHERE struct_element(if(k > 0, named_struct('f', unhex('FF')), named_struct('f', 'x')), 'f') = s
            UNION ALL
            SELECT 'utf8', k FROM fold_constant_by_be_invalid_utf8_t WHERE s = unhex('C3A9')
        """
        "order_qt_project_${mode}" """
            SELECT k AS project_${mode}, hex(concat(s, unhex('FF'))), hex(concat(s, unhex('C3A9')))
            FROM fold_constant_by_be_invalid_utf8_t
        """
    }

    sql "set enable_fold_constant_by_be = true"
    sql "set debug_skip_fold_constant = false"
    explain {
        sql "SELECT k FROM fold_constant_by_be_invalid_utf8_t WHERE s = unhex('C3A9')"
        contains "= 'é')"
    }
    explain {
        sql "SELECT k FROM fold_constant_by_be_invalid_utf8_t WHERE s = unhex('FF')"
        contains "= unhex('FF'))"
    }
}
