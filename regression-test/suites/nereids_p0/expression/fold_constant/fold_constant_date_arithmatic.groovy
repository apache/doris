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

suite("fold_constant_date_arithmatic") {
    def db = "fold_constant_date_arithmatic"
    sql "create database if not exists ${db}"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    testFoldConst("select substr(now(), 1, 10);")
    testFoldConst("select substr(now(3), 1, 10);")
    testFoldConst("select substr(curdate(), 1, 10);")
    testFoldConst("select substr(current_date(), 1, 10);")
    testFoldConst("select substr(current_timestamp(), 1, 10);")
    testFoldConst("select substr(current_timestamp(3), 1, 10);")

    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%h');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%I');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%l');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%r');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%h');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%I');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%l');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%r');")
    
    testFoldConst("select str_to_date('2023-02-29', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('1900-02-29', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('2025-04-31', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('31-12-2020 23:59:59', '%d-%m-%Y %H:%i:%s');")
    testFoldConst("select str_to_date('2020-12-31T23:59:59', '%Y-%m-%dT%H:%i:%s');")
    testFoldConst("select str_to_date('20201231235959', '%Y%m%d%H%i%s');")
    testFoldConst("select str_to_date('31/12/2020 23:59', '%d/%m/%Y %H:%i');")
    testFoldConst("select str_to_date('31/12/2020 11:59 PM', '%d/%m/%Y %h:%i %p');")
    testFoldConst("select str_to_date('20201231T235959', '%Y%m%dT%H%i%s');")
}
