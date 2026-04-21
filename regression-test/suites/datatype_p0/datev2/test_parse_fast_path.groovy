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

suite("test_parse_fast_path") {
    sql "set time_zone = '+00:00'"

    def result = sql """
        select cast(cast('2024-01-02' as date) as string),
               cast(cast('2024-01-02' as datev2) as string)
    """
    assertEquals("2024-01-02", result[0][0].toString())
    assertEquals("2024-01-02", result[0][1].toString())

    result = sql """
        select cast(cast('2024-01-02 03:04:05' as date) as string),
               cast(cast('2024-01-02 03:04:05 +08:00' as datev2) as string)
    """
    assertEquals("2024-01-02", result[0][0].toString())
    assertEquals("2024-01-02", result[0][1].toString())

    result = sql """
        select cast(cast('2024-01-02 03:04:05' as datetime) as string),
               cast(cast('2024-01-02 03:04:05.123456' as datetimev2(6)) as string)
    """
    assertEquals("2024-01-02 03:04:05", result[0][0].toString())
    assertEquals("2024-01-02 03:04:05.123456", result[0][1].toString())

    result = sql """
        select cast(cast('2024-01-02 03:04:05 +08:00' as datetime) as string),
               cast(cast('2024-01-02 03:04:05 +08:00' as timestamptz) as string)
    """
    assertEquals("2024-01-01 19:04:05", result[0][0].toString())
    assertEquals("2024-01-01 19:04:05+00:00", result[0][1].toString())

    test {
        sql """ select cast('2024-01-02 03:04:60' as datev2) """
        exception ""
    }
}
