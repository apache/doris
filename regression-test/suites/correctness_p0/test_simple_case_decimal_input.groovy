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

suite("test_simple_case_decimal_input") {
    sql "SET enable_decimal256 = true"
    sql "SET enable_strict_cast = false"
    sql "DROP TABLE IF EXISTS simple_case_decimal_input"
    sql """CREATE TABLE simple_case_decimal_input (
        id INT NOT NULL, s VARCHAR(20) NULL, k INT NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO simple_case_decimal_input VALUES
        (1, '1', 1), (2, '01', 1), (3, '1.0', 1), (4, '+1', 1), (5, ' 1', 1),
        (6, NULL, NULL), (7, 'other', NULL), (8, '2', 2), (9, '-1', -1), (10, '1.5', 3)"""
    sql "DROP TABLE IF EXISTS simple_case_native_input"
    sql """CREATE TABLE simple_case_native_input (
        id INT NOT NULL, k8 TINYINT NULL, k16 SMALLINT NULL, k64 BIGINT NULL, str STRING NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO simple_case_native_input VALUES
        (1, 1, 1, 1, 'one'), (2, 2, 2, 2, 'two'), (3, -1, -1, -1, ''),
        (4, NULL, NULL, NULL, NULL), (5, 127, 32767, 9223372036854775807, 'other')"""

    for (boolean simpleCase : [false, true]) {
        sql "SET enable_experimental_simple_case = ${simpleCase}"
        for (boolean shortCircuit : [false, true]) {
            sql "SET short_circuit_evaluation = ${shortCircuit}"
            def mode = "simple_${simpleCase}_short_${shortCircuit}"
            for (int precision : [9, 18, 38, 76]) {
                def input = "CAST(s AS DECIMAL(${precision}, 6))"
                def one = "CAST(1 AS DECIMAL(${precision}, 6))"
                def two = "CAST(2 AS DECIMAL(${precision}, 6))"
                quickTest("${mode}_decimal_${precision}", """SELECT id,
                    CASE ${input} WHEN ${one} THEN 11 WHEN ${one} THEN 99
                        WHEN ${two} THEN 22 ELSE id * 10 END,
                    CASE ${input} WHEN ${one} THEN 11 WHEN ${two} THEN 22 END,
                    CASE ${input} WHEN ${one} THEN 'one' WHEN ${two} THEN 'two' ELSE 'miss' END,
                    CASE ${input} WHEN ${one} THEN NULL WHEN ${two} THEN id ELSE k END
                    FROM simple_case_decimal_input ORDER BY id""")
                quickTest("${mode}_all_null_${precision}", """SELECT id,
                    CASE ${input} WHEN ${one} THEN 11 WHEN ${two} THEN 22 ELSE id END,
                    CASE ${input} WHEN ${one} THEN 11 WHEN ${two} THEN 22 END
                    FROM simple_case_decimal_input WHERE id IN (6, 7) ORDER BY id""")
                quickTest("${mode}_empty_${precision}", """SELECT
                    COUNT(CASE ${input} WHEN ${one} THEN 11 WHEN ${two} THEN 22 END)
                    FROM simple_case_decimal_input WHERE id < 0""")
                explain {
                    sql """SELECT CASE ${input} WHEN ${one} THEN id
                        WHEN ${two} THEN id + 1 ELSE id + 2 END AS result
                        FROM simple_case_decimal_input"""
                    contains "CASE "
                    if (simpleCase && !shortCircuit) {
                        notContains "CASE WHEN"
                    } else {
                        contains "CASE WHEN"
                    }
                }
            }
            // Keep each CASE in a separate projection query: sharing equality predicates
            // across CASE expressions can make CSE extract them before translation.
            def directCases = [:]
            for (int precision : [9, 18, 38, 76]) {
                def input = "CAST(s AS DECIMAL(${precision}, 6))"
                def one = "CAST(1 AS DECIMAL(${precision}, 6))"
                def two = "CAST(2 AS DECIMAL(${precision}, 6))"
                directCases["direct_decimal_${precision}"] =
                        "CASE ${input} WHEN ${one} THEN 11 WHEN ${one} THEN 99 WHEN ${two} THEN 22 ELSE id END"
                directCases["direct_decimal_${precision}_no_else"] =
                        "CASE ${input} WHEN ${one} THEN 11 WHEN ${two} THEN 22 END"
                directCases["direct_decimal_${precision}_nullable"] =
                        "CASE ${input} WHEN ${one} THEN NULL WHEN ${two} THEN id ELSE k END"
            }
            directCases["direct_integer"] = "CASE k WHEN 1 THEN id WHEN 1 THEN 99 WHEN 2 THEN id + 1 ELSE k END"
            directCases["direct_string"] = "CASE s WHEN '1' THEN 'first' WHEN '1' THEN 'duplicate' WHEN '2' THEN 'two' ELSE s END"
            directCases["direct_string_no_else"] = "CASE s WHEN '1' THEN id WHEN '2' THEN id + 1 END"
            directCases["direct_array"] = "CASE k WHEN 1 THEN array(id, 1) WHEN 2 THEN array(id, 2) ELSE array(id) END"
            directCases["direct_array_no_else"] = "CASE k WHEN 1 THEN array(id, 1) WHEN 2 THEN array(id, 2) END"
            directCases["direct_map"] = "CASE k WHEN 1 THEN map('a', id) WHEN 2 THEN map('b', id) ELSE map('c', id) END"
            directCases["direct_map_no_else"] = "CASE k WHEN 1 THEN map('a', id) WHEN 2 THEN map('b', id) END"
            directCases["direct_struct"] = "CASE k WHEN 1 THEN named_struct('a', id) WHEN 2 THEN named_struct('a', id + 1) ELSE named_struct('a', k) END"
            directCases["direct_struct_no_else"] = "CASE k WHEN 1 THEN named_struct('a', id) WHEN 2 THEN named_struct('a', id + 1) END"
            directCases.each { name, expression ->
                def query = "SELECT id, ${expression} AS result FROM simple_case_decimal_input ORDER BY id"
                quickTest("${mode}_${name}", query)
                explain {
                    sql query
                    contains "CASE "
                    if (simpleCase && !shortCircuit) {
                        notContains "CASE WHEN"
                    } else {
                        contains "CASE WHEN"
                    }
                }
                quickTest("${mode}_${name}_null_input", """SELECT id, ${expression} AS result
                    FROM simple_case_decimal_input WHERE id IN (6, 7) ORDER BY id""")
            }
            quickTest("${mode}_integer", """SELECT id,
                CASE k WHEN 1 THEN 11 WHEN 1 THEN 99 WHEN 2 THEN 22 ELSE 33 END,
                CASE k WHEN 1 THEN k WHEN 2 THEN NULL END,
                CASE k WHEN 1 THEN array(id, 1) WHEN 2 THEN array(id, 2) ELSE array(id) END,
                CASE k WHEN 1 THEN map('a', id) WHEN 2 THEN map('b', id) ELSE map('c', id) END,
                CASE k WHEN 1 THEN named_struct('a', id) WHEN 2 THEN named_struct('a', id + 1)
                    ELSE named_struct('a', k) END
                FROM simple_case_decimal_input ORDER BY id""")
            quickTest("${mode}_complex_no_else", """SELECT id,
                CASE k WHEN 1 THEN array(id, 1) WHEN 2 THEN array(id, 2) END,
                CASE k WHEN 1 THEN map('a', id) WHEN 2 THEN map('b', id) END,
                CASE k WHEN 1 THEN named_struct('a', id) WHEN 2 THEN named_struct('a', id + 1) END
                FROM simple_case_decimal_input ORDER BY id""")
            quickTest("${mode}_complex_all_null", """SELECT id,
                CASE k WHEN 1 THEN array(id, 1) WHEN 2 THEN array(id, 2) ELSE array(id) END,
                CASE k WHEN 1 THEN map('a', id) WHEN 2 THEN map('b', id) ELSE map('c', id) END,
                CASE k WHEN 1 THEN named_struct('a', id) WHEN 2 THEN named_struct('a', id + 1)
                    ELSE named_struct('a', k) END
                FROM simple_case_decimal_input WHERE id IN (6, 7) ORDER BY id""")
            quickTest("${mode}_string", """SELECT id,
                CASE s WHEN '1' THEN 'first' WHEN '1' THEN 'duplicate'
                    WHEN '2' THEN 'two' ELSE s END,
                CASE s WHEN 'other' THEN NULL WHEN '2' THEN 'two' END,
                CASE s WHEN NULL THEN 99 WHEN '1' THEN 11 ELSE 22 END
                FROM simple_case_decimal_input ORDER BY id""")
            // Mixed numeric/string comparison must retain the original coercion.
            quickTest("${mode}_coercion", """SELECT id,
                CASE s WHEN 1 THEN 11 WHEN 2 THEN 22 ELSE 33 END
                FROM simple_case_decimal_input ORDER BY id""")
            for (String input : ["k8", "k16", "k64"]) {
                for (boolean hasElse : [false, true]) {
                    def suffix = hasElse ? " ELSE id + 30" : ""
                    def query = """SELECT id,
                        CASE ${input} WHEN 1 THEN id + 10 WHEN 2 THEN id + 20${suffix} END AS result
                        FROM simple_case_native_input ORDER BY id"""
                    quickTest("${mode}_${input}_else_${hasElse}", query)
                    explain {
                        sql query
                        contains "CASE "
                        if (simpleCase && !shortCircuit) {
                            notContains "CASE WHEN"
                        } else {
                            contains "CASE WHEN"
                        }
                    }
                }
            }
            quickTest("${mode}_native_string", """SELECT id,
                CASE str WHEN 'one' THEN 'first' WHEN 'two' THEN 'second'
                    WHEN '' THEN 'empty' ELSE str END,
                CASE str WHEN 'one' THEN id WHEN 'two' THEN id + 10 END
                FROM simple_case_native_input ORDER BY id""")
            for (int branches : [256, 257]) {
                def clauses = (-254..(branches - 255)).collect { key ->
                    "WHEN ${key} THEN id + ${key + 255}"
                }.join(" ")
                def query = """SELECT id, CASE k ${clauses} ELSE -id END AS result
                    FROM simple_case_decimal_input ORDER BY id"""
                quickTest("${mode}_branches_${branches}", query)
                explain {
                    sql query
                    contains "CASE "
                    if (simpleCase && !shortCircuit && branches == 256) {
                        notContains "CASE WHEN"
                    } else {
                        contains "CASE WHEN"
                    }
                }
            }
        }
    }
}
