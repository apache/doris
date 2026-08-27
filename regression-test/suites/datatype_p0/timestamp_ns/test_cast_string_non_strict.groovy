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

suite("test_cast_string_non_strict") {
    sql "set time_zone = '+08:00'"
    sql "set enable_strict_cast = false"

    def nonStrictFormats = '''
        select 1 as id, cast('2023$07$16T19:20:30.123+08:00' as timestamp_ns) as result
        union all
        select 2, cast('  2023-7-4T9-5-3.123456789Z  ' as timestamp_ns)
        union all
        select 3, cast('99.12.31 23.59.59+05:30' as timestamp_ns)
        union all
        select 4, cast('2000/01/01T00/00/00-230' as timestamp_ns)
        union all
        select 5, cast('85 1 1T0 0 0. cst' as timestamp_ns)
        union all
        select 6, cast('2024-02-29T23:59:59.999999 UTC' as timestamp_ns)
        union all
        select 7, cast('70-01-01T00:00:00+14' as timestamp_ns)
        union all
        select 8, cast('0023-1-1T1:2:3. -00:00' as timestamp_ns)
        union all
        select 9, cast('2023-1-1T1:2:3. -00:00' as timestamp_ns)
        union all
        select 10, cast('2025/06/15T00:00:00.0-0' as timestamp_ns)
        union all
        select 11, cast('2025/06/15T00:00:00.99999999999' as timestamp_ns)
        union all
        select 12, cast('2024-02-29T23-59-60ZULU' as timestamp_ns)
        union all
        select 13, cast('2024 12 31T121212.123456 America/New_York' as timestamp_ns)
        union all
        select 14, cast('123.123' as timestamp_ns)
        union all
        select 15, cast('12121' as timestamp_ns)
        order by id
    '''

    order_qt_non_strict_formats nonStrictFormats
    testFoldConst(nonStrictFormats)

    for (def skipFoldConstant : [true, false]) {
        sql "set debug_skip_fold_constant = ${skipFoldConstant}"
        sql "set enable_strict_cast = true"
        test {
            sql "select cast('2023\$07\$16T19:20:30.123+08:00' as timestamp_ns)"
            exception '2023$07$16T19:20:30.123+08:00'
        }
    }
}
