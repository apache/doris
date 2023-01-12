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

suite("test_cast_array_functions_by_literal") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // array functions only supported in vectorized engine
    test {
        // char to int is ok
        sql "select cast(cast('1' as char) as int)"

        // check return 1 rows
        rowNum 1

        result(
            [[1]]
        )
    }

    //  ========== cast scalar to array===========
    // string/varchar/char is allowed to convert to array
    // Nereids does't support array function
    // qt_sql1 "select cast(cast('x' as char) as array<char>)"
    // Nereids does't support array function
    // qt_sql2 "select cast(cast('x' as string) as array<string>)"
    // Nereids does't support array function
    // qt_sql3 "select cast(cast('x' as varchar) as array<int>)"
    // Nereids does't support array function
    // qt_sql4 "select cast('[1,2,3]' as array<int>)"
    // Nereids does't support array function
    // qt_sql5 "select cast('[]' as array<int>)"
    // Nereids does't support array function
    // qt_sql6 """select cast('["a", "b", "c"]' as array<int>)"""
    // Nereids does't support array function
    // qt_sql7 """select cast('["a", "b", "c"]' as array<string>)"""

    // Nereids does't support array function
    // test {
    //     sql "select cast(NULL as array<int>)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(1 as array<int>)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(999.999 as array<double>)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(cast(999.999 as double) as array<double>)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(cast(999.999 as decimal) as array<decimal>)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }
    //  ========== cast array to scalar ===========

    // Nereids does't support array function
    // test {
    //     sql "select cast(['x'] as char)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(['x'] as varchar)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast(['x'] as string)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast([0] as int)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast([999.999] as double)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }

    // Nereids does't support array function
    // test {
    //     sql "select cast([999.999] as decimal)"
    //     // check exception message contains
    //     exception "errCode = 2,"
    // }
}
