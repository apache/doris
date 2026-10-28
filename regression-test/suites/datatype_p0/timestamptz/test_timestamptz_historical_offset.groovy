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

suite("test_timestamptz_historical_offset") {
    def originalZone = sql("select @@time_zone")[0][0]
    def originalStrict = sql("select @@enable_strict_cast")[0][0]
    def cases = [
        ["Asia/Shanghai", "1890-01-01 00:00:00.123456+00:00", "1890-01-01 08:05:43.123456+08:05:43"],
        ["America/New_York", "1880-01-01 00:00:00.123456+00:00", "1879-12-31 19:03:58.123456-04:56:02"],
        ["Asia/Shanghai", "2024-01-01 00:00:00.123456+00:00", "2024-01-01 08:00:00.123456+08:00"],
        ["America/New_York", "2024-01-01 00:00:00.123456+00:00", "2023-12-31 19:00:00.123456-05:00"],
        ["Asia/Kathmandu", "2024-01-01 00:00:00.123456+00:00", "2024-01-01 05:45:00.123456+05:45"]
    ]
    try {
        for (def testCase : cases) {
            sql "set time_zone = '${testCase[0]}'"
            for (def strict : [false, true]) {
                sql "set enable_strict_cast = ${strict}"
                // A nonconstant input exercises BE protocol formatting and parsing instead of
                // FE constant folding. The offset must retain the instant when sent back by a client.
                def wire = sql("""
                    select cast(concat('${testCase[1]}', substring(cast(number as string), 2))
                                as timestamptz(6))
                    from numbers('number' = '1')
                """)[0][0].toString()
                assertEquals(testCase[2], wire)
                def roundTrip = sql("""
                    select cast(concat('${wire}', substring(cast(number as string), 2))
                                as timestamptz(6)) = cast('${testCase[1]}' as timestamptz(6))
                    from numbers('number' = '1')
                """)
                assertEquals([[true]], roundTrip)
            }
        }
    } finally {
        sql "set time_zone = '${originalZone}'"
        sql "set enable_strict_cast = ${originalStrict}"
    }
}
