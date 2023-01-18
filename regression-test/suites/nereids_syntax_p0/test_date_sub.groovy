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

suite("test_date_sub") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    qt_select "select DAYS_SUB(cast('2020-01-01' as date), interval 2 year)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datev2),interval 2 year)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datetime), interval 2 year)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datetimev2), interval 2 year)"
    qt_select "select DAYS_SUB('2020-01-01' , interval 2 year)"
    
    qt_select "select DAYS_SUB(cast('2020-01-01' as date), interval 2 minute)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datev2),interval 2 minute)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datetime), interval 2 minute)"
    qt_select "select DAYS_SUB(cast('2020-01-01' as datetimev2), interval 2 minute)"
    qt_select "select DAYS_SUB('2020-01-01' , interval 2 minute)"

    qt_select "SELECT DAYS_SUB('2020-01-01', interval 2 day)"
    qt_select "SELECT DAYS_SUB('2020-01-01', interval 2 hour)"
    qt_select "SELECT DAYS_SUB('2020-01-01', interval 2 minute)"
    qt_select "SELECT DAYS_SUB('2020-01-01', interval 2 second)"
    qt_select "SELECT DATE_SUB('2020-01-01', interval 2 day)"
    qt_select "SELECT SUBDATE('2020-01-01', interval 2 day)"
    qt_select "SELECT DAYS_SUB('2020-01-01', 2)"
    qt_select "SELECT DAYS_SUB('2020-01-01', -4)"
        
    qt_select "SELECT YEARS_SUB('2020-01-01', 2)"
    qt_select "SELECT YEARS_SUB('2020-01-01', -4)"

    qt_select "SELECT MONTHS_SUB('2020-01-01', 2)"
    qt_select "SELECT MONTHS_SUB('2020-01-01', -4)"

    qt_select "SELECT WEEKS_SUB('2020-01-01', 2)"
    qt_select "SELECT WEEKS_SUB('2020-01-01', -4)"

    qt_select "SELECT HOURS_SUB('2020-01-01', 2)"
    qt_select "SELECT HOURS_SUB('2020-01-01', -4)"

    qt_select "SELECT MINUTES_SUB('2020-01-01', 2)"
    qt_select "SELECT MINUTES_SUB('2020-01-01', -4)"

    qt_select "SELECT SECONDS_SUB('2020-01-01', 2)"
    qt_select "SELECT SECONDS_SUB('2020-01-01', -4)"

    // TIMESTAMPDIFF
    qt_sql """ SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55') """
    qt_sql """ SELECT TIMESTAMPDIFF(SECOND,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(HOUR,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(DAY,'2003-02-01','2003-05-01') """
    qt_sql """ SELECT TIMESTAMPDIFF(WEEK,'2003-02-01','2003-05-01') """

}