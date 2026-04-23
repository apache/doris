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

suite("test_pythonudf_inline_complex") {
    // Test complex Python UDF using Inline mode
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: Array processing
        sql """ DROP FUNCTION IF EXISTS py_array_sum(ARRAY<INT>); """
        sql """
        CREATE FUNCTION py_array_sum(ARRAY<INT>) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(arr):
    if arr is None:
        return None
    return sum(arr)
\$\$;
        """
        
        qt_select_array_sum """ SELECT py_array_sum([1, 2, 3, 4, 5]) AS result; """
        
        // Test 2: String processing - reverse
        sql """ DROP FUNCTION IF EXISTS py_reverse_string(STRING); """
        sql """
        CREATE FUNCTION py_reverse_string(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    if s is None:
        return None
    return s[::-1]
\$\$;
        """
        
        qt_select_reverse """ SELECT py_reverse_string('Hello') AS result; """
        
        // Test 3: Multi-parameter complex calculation
        sql """ DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_weighted_avg(DOUBLE, DOUBLE, DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(val1, weight1, val2, weight2):
    if any(x is None for x in [val1, weight1, val2, weight2]):
        return None
    total_weight = weight1 + weight2
    if total_weight == 0:
        return None
    return (val1 * weight1 + val2 * weight2) / total_weight
\$\$;
        """
        
        qt_select_weighted_avg """ SELECT py_weighted_avg(80.0, 0.6, 90.0, 0.4) AS result; """
        
        // Test 4: String formatting
        sql """ DROP FUNCTION IF EXISTS py_format_name(STRING, STRING); """
        sql """
        CREATE FUNCTION py_format_name(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(first_name, last_name):
    if first_name is None or last_name is None:
        return None
    return f"{last_name.upper()}, {first_name.capitalize()}"
\$\$;
        """
        
        qt_select_format_name """ SELECT py_format_name('john', 'doe') AS result; """
        
        // Test 5: Numeric range validation
        sql """ DROP FUNCTION IF EXISTS py_in_range(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_in_range(INT, INT, INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(value, min_val, max_val):
    if any(x is None for x in [value, min_val, max_val]):
        return None
    return min_val <= value <= max_val
\$\$;
        """
        
        qt_select_in_range_true """ SELECT py_in_range(50, 0, 100) AS result; """
        qt_select_in_range_false """ SELECT py_in_range(150, 0, 100) AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_array_sum(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS py_reverse_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, DOUBLE, DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_format_name(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_in_range(INT, INT, INT);")
    }
}
