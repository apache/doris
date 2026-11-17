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

suite("test_string_literal_arithmetic") {
    // Use literals only. No table or stored row is required. The original failure is 'abc' + 1.
    // Save the variables so this suite does not change the next suite's execution mode.
    def originalSettings = ["enable_strict_cast", "enable_fold_constant_by_be",
            "debug_skip_fold_constant", "enable_sql_cache"].collectEntries { name ->
        [(name): sql("show variables like '${name}'")[0][1]]
    }
    try {
        // Disable the cache so each query tests the selected CAST and execution modes.
        sql "set enable_sql_cache=false"
        sql "set enable_fold_constant_by_be=false"
        for (skipFold in [false, true]) {
            // false tests FE constant folding. true tests BE evaluation of the same literals.
            sql "set debug_skip_fold_constant=${skipFold}"
            sql "set enable_strict_cast=false"

            // An invalid numeric string becomes NULL. Check both operand positions.
            order_qt_invalid_add "select 'abc' + 1, 1 + 'abc'"
            // These operators share the type conversion code used by addition.
            order_qt_invalid_shared """select 'abc' - 1, 1 - 'abc', 'abc' * 1, 1 * 'abc',
                    'abc' % 1, 1 % 'abc', 'abc' & 1, 1 & 'abc',
                    'abc' | 1, 1 | 'abc', 'abc' ^ 1, 1 ^ 'abc'"""
            // Division and explicit CAST already use the required conversion rules.
            order_qt_existing_paths "select 'abc' / 1, 'abc' div 1, cast('abc' as double) + 1"
            // Two string operands and an empty string must also reach CAST evaluation.
            order_qt_string_pairs "select 'abc' + '1', '' + 1"

            for (strict in [false, true]) {
                sql "set enable_strict_cast=${strict}"
                // Keep integer type selection. Fractional and exponent forms use DOUBLE.
                // Spaces are valid for DOUBLE, but the old BigDecimal check rejected them.
                order_qt_valid_numbers "select '1' + 1, '1.5' + 1, '1e1' + 1, ' 1 ' + 1"
            }

            // Strict mode must still fail. Match the input value because FE and BE use
            // different CAST error messages. Test each operator and operand position alone.
            for (op in ["+", "-", "*", "%", "&", "|", "^"]) {
                test {
                    sql "select 'abc' ${op} 1"
                    exception "abc"
                }
                test {
                    sql "select 1 ${op} 'abc'"
                    exception "abc"
                }
            }
        }
    } finally {
        originalSettings.each { name, value -> sql "set ${name}=${value}" }
    }
}
