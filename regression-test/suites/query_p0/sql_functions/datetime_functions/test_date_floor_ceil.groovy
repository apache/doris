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

suite("test_date_floor_ceil") {
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"
    sql "set enable_fold_constant_by_be=false;"

    qt_date_floor_datetime_1 """select date_floor("2023-07-14 10:51:11",interval 5 second); """
    qt_date_floor_datetime_2 """select date_floor("2023-07-14 10:51:00",interval 5 minute); """
    qt_date_floor_datetime_3 """select date_floor("2023-07-14 10:51:00",interval 5 hour); """
    qt_date_floor_datetime_4 """select date_floor("2023-07-14 10:51:00",interval 5 day);   """
    qt_date_floor_datetime_5 """select date_floor("2023-07-14 10:51:00",interval 5 month); """
    qt_date_floor_datetime_6 """select date_floor("2023-07-14 10:51:00",interval 5 year); """

    qt_date_ceil_datetime_1 """select date_ceil("2023-07-14 10:51:11",interval 5 second); """
    qt_date_ceil_datetime_2 """select date_ceil("2023-07-14 10:51:00",interval 5 minute); """
    qt_date_ceil_datetime_3 """select date_ceil("2023-07-14 10:51:00",interval 5 hour); """
    qt_date_ceil_datetime_4 """select date_ceil("2023-07-14 10:51:00",interval 5 day);   """
    qt_date_ceil_datetime_5 """select date_ceil("2023-07-14 10:51:00",interval 5 month); """
    qt_date_ceil_datetime_6 """select date_ceil("2023-07-14 10:51:00",interval 5 year); """

    qt_date_floor_date_1 """select date_floor("2023-07-14 10:51:11",interval 5 second); """
    qt_date_floor_date_2 """select date_floor("2023-07-14 10:51:00",interval 5 minute); """
    qt_date_floor_date_3 """select date_floor("2023-07-14 10:51:00",interval 5 hour); """
    qt_date_floor_date_4 """select date_floor("2023-07-14 10:51:00",interval 5 day);   """
    qt_date_floor_date_5 """select date_floor("2023-07-14 10:51:00",interval 5 month); """
    qt_date_floor_date_6 """select date_floor("2023-07-14 10:51:00",interval 5 year); """

    qt_date_ceil_date_1 """select date_ceil("2023-07-14 10:51:11",interval 5 second); """
    qt_date_ceil_date_2 """select date_ceil("2023-07-14 10:51:00",interval 5 minute); """
    qt_date_ceil_date_3 """select date_ceil("2023-07-14 10:51:00",interval 5 hour); """
    qt_date_ceil_date_4 """select date_ceil("2023-07-14 10:51:00",interval 5 day);   """
    qt_date_ceil_date_5 """select date_ceil("2023-07-14 10:51:00",interval 5 month); """
    qt_date_ceil_date_6 """select date_ceil("2023-07-14 10:51:00",interval 5 year); """

    // test hour_floor
    qt_hour_floor_fold_by_fe_1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_hour_floor_fold_by_fe_2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_hour_floor_fold_by_fe_3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    sql """set debug_skip_fold_constant = true"""
    qt_hour_floor_not_fold_1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_hour_floor_not_fold_2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_hour_floor_not_fold_3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    

    sql """set debug_skip_fold_constant = false"""
    qt_date_floor_corner_case_1 """ select date_floor('9999-12-31 23:59:59.999999', interval 5 minute); """
    qt_date_floor_corner_case_2 """ select date_floor('9999-12-31 23:59:59.999999', interval 33333 year); """
    qt_date_floor_corner_case_3 """ select date_floor('9999-12-31 23:59:59.999999', interval -10 year); """
    qt_date_floor_corner_case_4 """ select date_floor('1923-12-31 23:59:59.999999', interval -10 year); """
    // qt_date_floor_corner_case_5 """ select date_floor('0000-01-01 00:00:00', interval 7 minute); """//wrong
    qt_date_floor_corner_case_6 """ select date_floor('0001-01-01 00:00:00', interval 7 minute); """
    qt_date_ceil_corner_case_1 """ select date_ceil('9999-12-31 23:59:59.999999', interval 5 minute); """
    qt_date_ceil_corner_case_2 """ select date_ceil('9999-12-31 23:59:59.999999', interval 1 second); """
    qt_date_ceil_corner_case_3 """ select date_ceil('9999-12-31 23:59:59.999999', interval 100 year); """
    // qt_date_ceil_corner_case_4 """ select date_ceil('0000-01-01 23:59:59.999999', interval 7 month); """//wrong
    qt_date_ceil_corner_case_5 """ select date_ceil('0001-01-01 23:59:59.999999', interval 7 month); """
    qt_date_ceil_corner_case_6 """ select date_ceil('0001-09-01 23:59:59.999999', interval -7 month); """
    qt_date_ceil_corner_case_7 """ select date_ceil('0002-02-01 23:59:59.999999', interval -7 month); """
    qt_date_ceil_corner_case_8 """ select date_ceil('9999-12-31 23:54:59.999999', interval 5 minute); """
}
