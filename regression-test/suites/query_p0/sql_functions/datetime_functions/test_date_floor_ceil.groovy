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

    qt_sql1 """select date_floor("2023-07-14 10:51:11",interval 5 second); """
    qt_sql2 """select date_floor("2023-07-14 10:51:00",interval 5 minute); """
    qt_sql3 """select date_floor("2023-07-14 10:51:00",interval 5 hour); """
    qt_sql4 """select date_floor("2023-07-14 10:51:00",interval 5 day);   """
    qt_sql5 """select date_floor("2023-07-14 10:51:00",interval 5 month); """
    qt_sql6 """select date_floor("2023-07-14 10:51:00",interval 5 year); """

    qt_sql7 """select date_ceil("2023-07-14 10:51:11",interval 5 second); """
    qt_sql8 """select date_ceil("2023-07-14 10:51:00",interval 5 minute); """
    qt_sql9 """select date_ceil("2023-07-14 10:51:00",interval 5 hour); """
    qt_sql10 """select date_ceil("2023-07-14 10:51:00",interval 5 day);   """
    qt_sql11 """select date_ceil("2023-07-14 10:51:00",interval 5 month); """
    qt_sql12 """select date_ceil("2023-07-14 10:51:00",interval 5 year); """

    // test hour_floor
    qt_sql1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_sql2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_sql3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    sql """set debug_skip_fold_constant = true"""
    qt_sql1 """select hour_floor("2023-07-14 10:51:00", 5, "0001-01-01 00:00:00");"""
    qt_sql2 """select hour_floor("2023-07-14 10:51:00", 5, "1970-01-01 00:00:00");"""
    qt_sql3 """select hour_floor("2023-07-14 10:51:00", 5);"""
    

    sql """set debug_skip_fold_constant = false"""
    qt_x1 """ select date_floor('9999-12-31 23:59:59.999999', interval 5 minute); """
    qt_x2 """ select date_floor('9999-12-31 23:59:59.999999', interval 33333 year); """
    test {
        sql """ select date_floor('9999-12-31 23:59:59.999999', interval -10 year); """
        exception "Operation year_floor of 9999-12-31 23:59:59.999999, -10 input wrong parameters, period can`t be negative or zero"
    }
    test {
        sql """ select date_floor('1923-12-31 23:59:59.999999', interval -10 year); """
        exception "Operation year_floor of 1923-12-31 23:59:59.999999, -10 input wrong parameters, period can`t be negative or zero"
    }

    // qt_x5 """ select date_floor('0000-01-01 00:00:00', interval 7 minute); """//wrong
    qt_x6 """ select date_floor('0001-01-01 00:00:00', interval 7 minute); """
    
    test {
        sql """ select date_ceil('9999-12-31 23:59:59.999999', interval 5 minute); """
        exception "Operation minute_ceil of 9999-12-31 23:59:59.999999, 5 out of range"
    }

    test {
        sql """ select date_ceil('9999-12-31 23:59:59.999999', interval 1 second); """
        exception "Operation second_ceil of 9999-12-31 23:59:59.999999, 1 out of range"
    }
    test {
        sql """ select date_ceil('9999-12-31 23:59:59.999999', interval 100 year); """
        exception "Operation year_ceil of 9999-12-31 23:59:59.999999, 100 out of range"
    }
    // qt_x10 """ select date_ceil('0000-01-01 23:59:59.999999', interval 7 month); """//wrong
    qt_x11 """ select date_ceil('0001-01-01 23:59:59.999999', interval 7 month); """
    test {
        sql """ select date_ceil('0001-09-01 23:59:59.999999', interval -7 month); """
        exception "Operation month_ceil of 0001-09-01 23:59:59.999999, -7 input wrong parameters, period can`t be negative or zero"
    }
    test {
        sql """ select date_ceil('0002-02-01 23:59:59.999999', interval -7 month); """
        exception "Operation month_ceil of 0002-02-01 23:59:59.999999, -7 input wrong parameters, period can`t be negative or zero"
    }
    qt_x14 """ select date_ceil('9999-12-31 23:54:59.999999', interval 5 minute); """

    //All flow tests are capture error of out of bound
    test {
        sql """ select date_ceil('9999-12-31 23:59:59.999999', interval 5 year); """
        exception "Operation year_ceil of 9999-12-31 23:59:59.999999, 5 out of range"
    } 

    test {
        sql """ select date_ceil('9999-12-31 23:59:59.999999', interval 15 minute); """
        exception "Operation minute_ceil of 9999-12-31 23:59:59.999999, 15 out of range"
    }

    test {
        sql """ select date_ceil('9999-12-01 00:00:00', interval 5 month); """
        exception "Operation month_ceil of 9999-12-01 00:00:00, 5 out of range"
    }

    test {
        sql """ select year_ceil('9999-12-01 00:00:00', 5); """
        exception "Operation year_ceil of 9999-12-01 00:00:00, 5 out of range"
    }

    test {
        sql """  select month_ceil('9999-12-01 00:00:00', 5); """
        exception "Operation month_ceil of 9999-12-01 00:00:00, 5 out of range"
    }
    
    test {
        sql """ select day_ceil('9999-12-31 00:00:00', 5); """
        exception "Operation day_ceil of 9999-12-31 00:00:00, 5 out of range"
    }

    test {
        sql """ select hour_ceil('9999-12-31 23:59:59.999999', 5); """
        exception "Operation hour_ceil of 9999-12-31 23:59:59.999999, 5 out of range"
    }

    test {
        sql """ select minute_ceil('9999-12-31 23:59:59.999999', 5); """
        exception "Operation minute_ceil of 9999-12-31 23:59:59.999999, 5 out of range"
    }

    test {
        sql """ select second_ceil('9999-12-31 23:59:59.999999', 5); """
        exception "Operation second_ceil of 9999-12-31 23:59:59.999999, 5 out of range"
    }
}
