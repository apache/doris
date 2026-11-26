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

suite("test_timestamptz_null_string") {
    def originalZone = sql("select @@time_zone")[0][0]
    def originalStrict = sql("select @@enable_strict_cast")[0][0]
    def originalSkipFold = sql("select @@debug_skip_fold_constant")[0][0]
    try {
        sql "set time_zone = '+08:00'"
        sql "set enable_strict_cast = false"
        for (def skipFold : [false, true]) {
            sql "set debug_skip_fold_constant = ${skipFold}"
            // Invalid casts and NULL inputs leave no valid timestamp payload for the
            // following formatter; only the null map determines whether a row is readable.
            assertEquals([[null]], sql("""
                select cast(second_floor('9999-12-31 23:59:59.999999-02:00', 5) as string)
            """))
            assertEquals([[null]], sql("""
                select cast(second_floor(cast(null as timestamptz(6)), 5) as string)
            """))
            assertEquals([[null], ['2024-01-02 11:04:05.000000+08:00'],
                          [null], ['2024-01-02 11:04:05.000000+08:00']], sql("""
                select cast(second_floor(
                    if(number % 2 = 0, cast(null as timestamptz(6)),
                       cast('2024-01-02 03:04:05.123456+00:00' as timestamptz(6))), 5) as string)
                from numbers('number' = '4') order by number
            """))
        }
    } finally {
        sql "set time_zone = '${originalZone}'"
        sql "set enable_strict_cast = ${originalStrict}"
        sql "set debug_skip_fold_constant = ${originalSkipFold}"
    }
}
