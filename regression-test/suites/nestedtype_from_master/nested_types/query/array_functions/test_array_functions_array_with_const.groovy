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

suite("test_array_functions_array_with_const", "p0") {
    sql "set enable_nereids_planner=false;"
    //array_with_constant
    qt_old_sql "SELECT 'array_with_constant';"
    order_qt_old_sql "SELECT array_with_constant(3, number) FROM numbers limit 10;"
    order_qt_old_sql "SELECT array_with_constant(number, 'Hello') FROM numbers limit 10;"
    // not support const expression
//    order_qt_sql "SELECT array_with_constant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers limit 10;"
    // mistake:No matching function with signature: array_with_constant(INT, ARRAY<NULL_TYPE>)
//    order_qt_sql "SELECT array_with_constant(number, []) FROM numbers limit 10;"
    order_qt_old_sql "SELECT array_with_constant(2, 'qwerty'), array_with_constant(0, -1), array_with_constant(1, 1);"
    //  -- { serverError }
    try {
        sql """ 
                SELECT array_with_constant(-231.37104, -138); 
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size should in range(0, 1000000) in function"))
    }

    // -- {server for large array}
    try {
        sql """
                SELECT array_with_constant(1000001, 1);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size should in range(0, 1000000) in function"))
    }


    // nereids
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"

    //array_with_constant
    qt_nereid_sql "SELECT 'array_with_constant';"
    order_qt_nereid_sql "SELECT array_with_constant(3, number) FROM numbers limit 10;"
    order_qt_nereid_sql "SELECT array_with_constant(number, 'Hello') FROM numbers limit 10;"
    // not support const expression
//    order_qt_sql "SELECT array_with_constant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers limit 10;"
    order_qt_sql "SELECT array_with_constant(number, []) FROM numbers limit 10;"
    order_qt_nereid_sql "SELECT array_with_constant(2, 'qwerty'), array_with_constant(0, -1), array_with_constant(1, 1);"
    //  -- { serverError }
    try {
        sql """ 
                SELECT array_with_constant(-231.37104, -138); 
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size should in range(0, 1000000) in function"))
    }

    // -- {server for large array}
    try {
        sql """
                SELECT array_with_constant(1000001, 1);
                """
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Array size should in range(0, 1000000) in function"))
    }
}
