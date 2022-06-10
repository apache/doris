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


suite("test_time_round") {
   
    sql """ set enable_vectorized_engine = false """ 
    // fix by issues/9711, expect: '1970-01-01T01:00:30' 
    qt_select "select hour_ceil('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '1970-01-01T00:00:30'
    qt_select "select hour_floor('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '2022-05-25'
    qt_select "select day_ceil('2022-05-25')"

    // fix by issues/9711, expect: '2022-05-01'
    qt_select "select month_ceil('2022-05-01')"

    qt_select "select minute_ceil('2022-05-25 00:05:10')"
    qt_select "select minute_floor('2022-05-25 00:05:10')"
    qt_select "select day_ceil('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select day_floor('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00')"
    qt_select "select month_ceil('2022-05-25 00:00:00')"
    qt_select "select month_floor('2022-05-25 00:00:00')"
    qt_select "select year_ceil('2022-05-25 00:00:00')"
    qt_select "select year_floor('2022-05-25 00:00:00')"


    sql """ set enable_vectorized_engine = true """
    // fix by issues/9711, expect: '1970-01-01T01:00:30' 
    qt_select "select hour_ceil('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '1970-01-01T00:00:30'
    qt_select "select hour_floor('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '2022-05-25'
    qt_select "select day_ceil('2022-05-25')"

    // fix by issues/9711, expect: '2022-05-01'
    qt_select "select month_ceil('2022-05-01')"

    qt_select "select minute_ceil('2022-05-25 00:05:10')"
    qt_select "select minute_floor('2022-05-25 00:05:10')"
    qt_select "select day_ceil('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select day_floor('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00')"
    qt_select "select month_ceil('2022-05-25 00:00:00')"
    qt_select "select month_floor('2022-05-25 00:00:00')"
    qt_select "select year_ceil('2022-05-25 00:00:00')"
    qt_select "select year_floor('2022-05-25 00:00:00')"

}
