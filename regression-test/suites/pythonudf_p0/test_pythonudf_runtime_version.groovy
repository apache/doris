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

suite("test_pythonudf_runtime_version") {
    // Test different configurations of runtime_version parameter
    
    // Disabled temporarily
    return
    
    try {
        // Test 1: Specify short version number (x.xx format) with inline code
        sql """ DROP FUNCTION IF EXISTS py_version_test_short(INT); """
        sql """
        CREATE FUNCTION py_version_test_short(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "3.12"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x * 2
\$\$;
        """
        
        qt_select_version_short """ SELECT py_version_test_short(21) AS result; """
        
        // Test 2: Specify full version number (x.xx.xx format) with inline code
        sql """ DROP FUNCTION IF EXISTS py_version_test_full(INT); """
        sql """
        CREATE FUNCTION py_version_test_full(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "3.12.10"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x * 3
\$\$;
        """
        
        qt_select_version_full """ SELECT py_version_test_full(10) AS result; """
        
        // Test 3: Do not specify runtime_version (use default)
        sql """ DROP FUNCTION IF EXISTS py_version_test_default(INT); """
        sql """
        CREATE FUNCTION py_version_test_default(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 100
\$\$;
        """
        
        qt_select_version_default """ SELECT py_version_test_default(50) AS result; """
        
        // Test 4: String function with runtime_version
        sql """ DROP FUNCTION IF EXISTS py_version_string_test(STRING); """
        sql """
        CREATE FUNCTION py_version_string_test(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "3.12"
        )
        AS \$\$
def evaluate(s):
    if s is None:
        return None
    return s.upper()
\$\$;
        """
        
        qt_select_version_string """ SELECT py_version_string_test('hello') AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_version_test_short(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_version_test_full(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_version_test_default(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_version_string_test(STRING);")
    }
}
