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

    order_qt_date_fast_path """
        select cast('2024-01-02' as date), cast('2024-01-02' as datev2)
    """

    order_qt_date_fast_path_fallback """
        select cast('2024-1-2' as date), cast('2024-1-2' as datev2)
    """

    order_qt_datetime_fast_path """
        select cast('2024-01-02 03:04:05' as datetime), cast('2024-01-02 03:04:05' as datetimev2(6))
    """

    order_qt_datetime_fast_path_suffix """
        select cast('2024-01-02 03:04:05.123456' as datetimev2(6)),
               cast('2024-01-02 03:04:05 +08:00' as datetime)
    """

    order_qt_datetime_fast_path_fallback """
        select cast('2024-1-2 03:04:05' as datetime), cast('2024-1-2 03:04:05' as datetimev2(6))
    """

    order_qt_timestamptz_fast_path """
        select cast('2024-01-02 03:04:05 +08:00' as timestamptz),
               cast('2024-01-02 03:04:05.123456 +08:00' as timestamptz)
    """

    order_qt_timestamptz_fast_path_fallback """
        select cast('2024-1-2 03:04:05 +08:00' as timestamptz)
    """
}
