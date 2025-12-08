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

suite("test_pythonudf_inline_basic") {
    // Test basic Python UDF using Inline mode
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: Simple integer addition
        sql """ DROP FUNCTION IF EXISTS py_add(INT, INT); """
        sql """
        CREATE FUNCTION py_add(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(a, b):
    return a + b
\$\$;
        """
        
        qt_select_add """ SELECT py_add(10, 20) AS result; """
        
        // Test 2: String concatenation
        sql """ DROP FUNCTION IF EXISTS py_concat(STRING, STRING); """
        sql """
        CREATE FUNCTION py_concat(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s1, s2):
    if s1 is None or s2 is None:
        return None
    return s1 + s2
\$\$;
        """
        
        qt_select_concat """ SELECT py_concat('Hello', ' World') AS result; """
        qt_select_concat_null """ SELECT py_concat('Hello', NULL) AS result; """
        
        // Test 3: Mathematical operations
        sql """ DROP FUNCTION IF EXISTS py_square(DOUBLE); """
        sql """
        CREATE FUNCTION py_square(DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x * x
\$\$;
        """
        
        qt_select_square """ SELECT py_square(5.0) AS result; """
        qt_select_square_negative """ SELECT py_square(-3.0) AS result; """
        
        // Test 4: Conditional logic
        sql """ DROP FUNCTION IF EXISTS py_is_positive(INT); """
        sql """
        CREATE FUNCTION py_is_positive(INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(num):
    if num is None:
        return None
    return num > 0
\$\$;
        """
        
        qt_select_positive """ SELECT py_is_positive(10) AS result; """
        qt_select_negative """ SELECT py_is_positive(-5) AS result; """
        qt_select_zero """ SELECT py_is_positive(0) AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_add(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_concat(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_square(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_is_positive(INT);")
    }
}
