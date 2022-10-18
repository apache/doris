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
    // open enable_array_type
    sql "ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true')"
    // array functions only supported in vectorized engine
    sql """ set enable_vectorized_engine = true """
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

    test {
        sql "select cast(cast('x' as char) as array<char>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast('x' as string) as array<string>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast('x' as varchar) as array<int>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(NULL as array<int>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(1 as array<int>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(999.999 as array<double>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast(999.999 as double) as array<double>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast(999.999 as decimal) as array<decimal>)"
        // check exception message contains
        exception "errCode = 2,"
    }
    //  ========== cast array to scalar ===========

    test {
        sql "select cast(['x'] as char)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(['x'] as varchar)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(['x'] as string)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([0] as int)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([999.999] as double)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([999.999] as decimal)"
        // check exception message contains
        exception "errCode = 2,"
    }

    // Not Vectorized Engine
    sql """ set enable_vectorized_engine = false """

    //  ========== cast scalar to array ===========

    test {
        sql "select cast(cast('x' as char) as array<char>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast('x' as string) as array<string>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(1 as array<int>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(999.999 as array<double>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast(999.999 as double) as array<double>)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(cast(999.999 as decimal) as array<decimal>)"
        // check exception message contains
        exception "errCode = 2,"
    }
    //  ========== cast array to scalar ===========

    test {
        sql "select cast(['x'] as char)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(['x'] as varchar)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast(['x'] as string)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([0] as int)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([999.999] as double)"
        // check exception message contains
        exception "errCode = 2,"
    }

    test {
        sql "select cast([999.999] as decimal)"
        // check exception message contains
        exception "errCode = 2,"
    }
}
