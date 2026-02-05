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

suite("fe_try_cast") {
    sql """set enable_fallback_to_original_planner=false"""
    sql """set debug_skip_fold_constant=false"""
    sql """set enable_strict_cast=true"""

    qt_datetime1("""select try_cast("2023-07-16T19:20:30.123+08:00" as datetime)""")
    explain {
        sql("select try_cast(\"2023-07-16T19:20:30.123+08:00\" as datetime)")
        contains "2023-07-16 19:20:30"
        notContains("TRYCAST")
    }

    qt_datetime2("""select try_cast("abc" as datetime)""")
    explain {
        sql("select try_cast(\"abc\" as datetime)")
        contains "NULL"
    }

    qt_datetime3("""select try_cast(1000000 as tinyint);""")
    explain {
        sql("select try_cast(1000000 as tinyint);")
        contains "NULL"
    }

    qt_datetime4("""select try_cast(123.456 as decimal(4, 2));""")
    explain {
        sql("select try_cast(123.456 as decimal(4, 2))")
        contains "NULL"
    }

    test {
        sql """select cast(true as date)"""
        exception "cannot cast BOOLEAN to DATEV2"
    }
}
