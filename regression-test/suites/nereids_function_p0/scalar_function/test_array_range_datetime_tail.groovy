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

suite("test_array_range_datetime_tail") {
    qt_day_partial """select array_range(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), cast('2024-01-02 12:00:00.000000' as datetimev2(6)))"""
    qt_day_exact """select array_range(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), cast('2024-01-02 00:00:00.000000' as datetimev2(6)))"""
    qt_day_empty """select array_range(cast('2024-01-02 00:00:00.000000' as datetimev2(6)), cast('2024-01-01 00:00:00.000000' as datetimev2(6)))"""
    order_qt_day_vector """select array_range(date_add(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), interval number day), cast('2024-01-02 12:00:00.000000' as datetimev2(6))) from numbers("number" = "2") order by number"""

    qt_second_partial """select sequence(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), cast('2024-01-01 00:00:01.500000' as datetimev2(6)), interval 1 second)"""
    qt_second_exact """select sequence(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), cast('2024-01-01 00:00:01.000000' as datetimev2(6)), interval 1 second)"""
    order_qt_second_vector """select sequence(date_add(cast('2024-01-01 00:00:00.000000' as datetimev2(6)), interval number second), cast('2024-01-01 00:00:01.500000' as datetimev2(6)), interval 1 second) from numbers("number" = "2") order by number"""

    qt_month_end """select array_range(cast('2020-01-31 00:00:00' as datetimev2(6)), cast('2020-03-30 00:00:00' as datetimev2(6)), interval 1 month)"""
    qt_upper_bound """select array_range(cast('9999-12-31 23:59:59.999998' as datetimev2(6)), cast('9999-12-31 23:59:59.999999' as datetimev2(6)), interval 1 second)"""
    qt_timestamp_ns """select sequence(cast('2024-01-01 00:00:00.000000001' as timestamp_ns), cast('2024-01-01 00:00:01.000000002' as timestamp_ns), interval 1 second)"""

    test {
        sql """select array_range(date_add(cast('2024-01-01 00:00:00' as datetimev2(6)), interval number second), cast('2024-01-12 13:46:41' as datetimev2(6)), interval 1 second) from numbers("number" = "1")"""
        exception "Array size exceeds the limit 1000000"
    }
}
