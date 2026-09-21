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

suite("test_timestamptz_output_boundary") {
    def originalZone = sql("select @@time_zone")[0][0]
    def originalStrict = sql("select @@enable_strict_cast")[0][0]
    def minimum = "0000-01-01 00:00:00.000000+00:00"
    def maximum = "9999-12-31 23:59:59.999999+00:00"
    def readTimestamp = { value ->
        // Nonconstant inputs reach the BE formatter instead of FE constant folding.
        """select cast(concat('${value}', substring(cast(number as string), 2)) as timestamptz(6))
           from numbers('number' = '1')"""
    }
    try {
        for (def strict : [false, true]) {
            sql "set enable_strict_cast = ${strict}"
            sql "set time_zone = '+08:00'"
            for (def target : [["datetimev2(6)", "to datetime in timezone"],
                               ["timestamptz(0)", "to timestamptz in timezone"]]) {
                // Failed casts must retain their error/NULL contract even when the input
                // cannot be displayed in the session timezone while reporting the error.
                def query = """
                    select cast(cast(concat('${maximum}', substring(cast(number as string), 2))
                                     as timestamptz(6)) as ${target[0]})
                    from numbers('number' = '1')
                """
                if (strict) {
                    test {
                        sql query
                        exception target[1]
                    }
                } else {
                    assertEquals([[null]], sql(query))
                }
            }
            for (def entry : [["-08:00", minimum], ["+08:00", maximum]]) {
                sql "set time_zone = '${entry[0]}'"
                test {
                    sql readTimestamp(entry[1])
                    exception "TIMESTAMPTZ local year is outside [0, 9999]"
                }
            }
            for (def entry : [["UTC", minimum, minimum], ["UTC", maximum, maximum],
                              ["+08:00", minimum, "0000-01-01 08:00:00.000000+08:00"],
                              ["-08:00", maximum, "9999-12-31 15:59:59.999999-08:00"]]) {
                sql "set time_zone = '${entry[0]}'"
                def wire = sql(readTimestamp(entry[1]))[0][0].toString()
                assertEquals(entry[2], wire)
                sql "set time_zone = 'UTC'"
                assertEquals(entry[1], sql(readTimestamp(wire))[0][0].toString())
            }
        }
    } finally {
        sql "set time_zone = '${originalZone}'"
        sql "set enable_strict_cast = ${originalStrict}"
    }
}
