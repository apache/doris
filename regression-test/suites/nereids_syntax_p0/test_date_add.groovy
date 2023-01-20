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

suite("test_date_add") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"
    
    qt_select "SELECT DAYS_ADD('2020-01-01', interval 2 hour)"
    qt_select "SELECT DAYS_ADD('2020-01-01', interval 2 minute)"
    qt_select "SELECT DAYS_ADD('2020-01-01', interval 2 second)"
    qt_select "SELECT DATE_ADD('2020-01-01', interval 2 day)"
    qt_select "SELECT ADDDATE('2020-01-01', interval 2 day)"
    qt_select "SELECT DAYS_ADD('2020-01-01', 2)"
    qt_select "SELECT DAYS_ADD('2020-01-01', -4)"
    
    qt_select "SELECT YEARS_ADD('2020-01-01', 2)"
    qt_select "SELECT YEARS_ADD('2020-01-01', -4)"

    qt_select "SELECT MONTHS_ADD('2020-01-01', 2)"
    qt_select "SELECT MONTHS_ADD('2020-01-01', -4)"

    qt_select "SELECT WEEKS_ADD('2020-01-01', 2)"
    qt_select "SELECT WEEKS_ADD('2020-01-01', -4)"

    qt_select "SELECT HOURS_ADD('2020-01-01', 2)"
    qt_select "SELECT HOURS_ADD('2020-01-01', -4)"

    qt_select "SELECT MINUTES_ADD('2020-01-01', 2)"
    qt_select "SELECT MINUTES_ADD('2020-01-01', -4)"

    qt_select "SELECT SECONDS_ADD('2020-01-01', 2)"
    qt_select "SELECT SECONDS_ADD('2020-01-01', -4)"

    qt_sql """ SELECT TIMESTAMPADD(YEAR,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(MONTH,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(WEEK,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(DAY,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(HOUR,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(MINUTE,1,'2003-02-01'); """
    qt_sql """ SELECT TIMESTAMPADD(SECOND,1,'2003-02-01'); """
}