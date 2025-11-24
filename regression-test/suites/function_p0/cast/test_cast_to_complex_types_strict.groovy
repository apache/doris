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

suite("test_cast_to_complex_types_strict") {
    sql "set debug_skip_fold_constant=true;"
    sql "set enable_strict_cast=true;"

    // ====== ARRAY type conversion - strict mode ======
    
    // Test converting valid string formats to ARRAY type
    qt_cast_str_to_array_valid """
    SELECT 
        CAST('[]' AS ARRAY<INT>) AS empty_array,
        CAST('[123, 456, 789]' AS ARRAY<INT>) AS standard_array,
        CAST('[  123  ,  456  ,  789  ]' AS ARRAY<INT>) AS array_with_spaces,
        CAST('["123", "456", "789"]' AS ARRAY<INT>) AS array_with_quotes,
        CAST('[null, 123, 456]' AS ARRAY<INT>) AS array_with_null;
    """

    // Test nested arrays
    qt_cast_str_to_nested_array """
    SELECT 
        CAST('[[1, 2], [3, 4], [5, 6]]' AS ARRAY<ARRAY<INT>>) AS nested_array,
        CAST('[[], [1], [1, 2]]' AS ARRAY<ARRAY<INT>>) AS nested_array_mixed;
    """
    
    // Test converting invalid string formats to ARRAY type (should error in strict mode)
    test {
        sql "SELECT CAST('  []' AS ARRAY<INT>) AS invalid_start;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('[ ]' AS ARRAY<INT>) AS empty_space_element;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('[1, abc, 3]' AS ARRAY<INT>) AS invalid_element;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('[1, 2, ]' AS ARRAY<INT>) AS trailing_comma;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('[1, , 3]' AS ARRAY<INT>) AS empty_element;"
        exception "INVALID_ARGUMENT"
    }

    // ====== MAP type conversion - strict mode ======
    
    // Test converting valid string formats to MAP type
    qt_cast_str_to_map_valid """
    SELECT 
        CAST('{}' AS MAP<INT, INT>) AS empty_map,
        CAST('{1:2, 3:4, 5:6}' AS MAP<INT, INT>) AS standard_map,
        CAST('{  1  :  2  ,  3  :  4  ,  5  :  6  }' AS MAP<INT, INT>) AS map_with_spaces,
        CAST('{"1":"2", "3":"4", "5":"6"}' AS MAP<INT, INT>) AS map_with_quotes,
        CAST('{1:null, 3:4}' AS MAP<INT, INT>) AS map_with_null;
    """
    
    // Test nested MAP
    qt_cast_str_to_nested_map """
    SELECT 
        CAST('{1:{11:12, 13:14}, 2:{21:22, 23:24}}' AS MAP<INT, MAP<INT, INT>>) AS nested_map;
    """
    
    // Test converting invalid string formats to MAP type (should error in strict mode)
    test {
        sql "SELECT CAST('  {}' AS MAP<INT, INT>) AS invalid_start;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('{1:2, a:4}' AS MAP<INT, INT>) AS invalid_key;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('{1:a, 3:4}' AS MAP<INT, INT>) AS invalid_value;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('{1:2, 3:4,}' AS MAP<INT, INT>) AS trailing_comma;"
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql "SELECT CAST('{1:2, 3, 5:6}' AS MAP<INT, INT>) AS invalid_format;"
        exception "INVALID_ARGUMENT"
    }
    
    // ====== STRUCT type conversion - strict mode ======

    
    // Test converting valid string formats to STRUCT type
    qt_cast_str_to_struct_valid """
    SELECT 
        CAST('{}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS empty_struct,
        CAST('{1, "hello"}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS values_only_struct,
        CAST('{"a":1, "b":"hello"}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS fields_values_struct,
        CAST('{  "a"  :  1  ,  "b"  :  "hello"  }' AS STRUCT<a:INT, b:VARCHAR(10)>) AS struct_with_spaces,
        CAST('{null, "hello"}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS struct_with_null;
    """
    
    // Test nested STRUCT
    qt_cast_str_to_nested_struct """
    SELECT 
        CAST('{"x":1, "y":{"m":2, "n":"nested"}}' AS STRUCT<x:INT, y:STRUCT<m:INT, n:VARCHAR(10)>>) AS nested_struct;
    """
    
    // Test converting invalid string formats to STRUCT type (should error in strict mode)
    test {
        sql """SELECT CAST('  {}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS invalid_start;"""
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql """SELECT CAST('{a:1, b:"hello"}' AS STRUCT<x:INT, y:VARCHAR(10)>) AS field_name_mismatch;"""
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql """SELECT CAST('{1, "hello", 3}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS too_many_values;"""
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql """SELECT CAST('{1}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS too_few_values;"""
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql """SELECT CAST('{"a":"abc", "b":"hello"}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS invalid_element;"""
        exception "INVALID_ARGUMENT"
    }
    
    test {
        sql """SELECT CAST('{"a":1, "b":"hello",}' AS STRUCT<a:INT, b:VARCHAR(10)>) AS trailing_comma;"""
        exception "INVALID_ARGUMENT"
    }
    
    // Test type conversion - from other types to complex types
    qt_cast_from_other_types """
    SELECT 
        CAST(ARRAY('123', '456') AS ARRAY<INT>) AS array_str_to_int,
        CAST(MAP('1', '2', '3','4') AS MAP<INT, INT>) AS map_str_to_int;
    """
}
