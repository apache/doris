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

suite("test_cast_to_datetime", "nonConcurrent") {
    // cast string of invalid datetime to datetime
    qt_cast_string_to_datetime_invalid0 """ select cast("627492340" as datetime); """
    qt_cast_string_to_datetime_invalid1 """ select cast("" as datetime); """
    qt_cast_string_to_datetime_invalid2 """ select cast("1" as datetime); """
    qt_cast_string_to_datetime_invalid3 """ select cast("a" as datetime); """
    qt_cast_string_to_datetime_invalid4 """ select cast("null" as datetime); """
    qt_cast_string_to_datetime_invalid5 """ select cast(null as datetime); """

    sql """ drop table if exists test_cast_to_datetime1; """
    sql """ drop table if exists test_cast_to_datetime2; """
    sql """ 
        create table test_cast_to_datetime1 (k1 int, k2 date) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        create table test_cast_to_datetime2 (k1 int, k2 date) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """ insert into test_cast_to_datetime2 values(1, null); """
    sql """ set enable_insert_strict=true; """
    sql """
        insert into test_cast_to_datetime1 select k1, cast(cast(k2 as char) as date) from test_cast_to_datetime2;
    """
    qt_select_cast_date1 """ select * from  test_cast_to_datetime1; """

    sql """ drop table if exists test_cast_to_datetime1; """
    sql """ drop table if exists test_cast_to_datetime2; """
    sql """
        create table test_cast_to_datetime1 (k1 int, k2 datetime) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        create table test_cast_to_datetime2 (k1 int, k2 datetime) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """ insert into test_cast_to_datetime2 values(1, null); """
    sql """
        insert into test_cast_to_datetime1 select k1, cast(cast(k2 as char) as datetime) from test_cast_to_datetime2;
    """
    qt_select_cast_date2 """ select * from  test_cast_to_datetime1; """

    sql """ ADMIN SET FRONTEND CONFIG ("enable_date_conversion" = "false"); """
    sql """ ADMIN SET FRONTEND CONFIG ("disable_datev1" = "false"); """

    sql """ drop table if exists test_cast_to_datetime1; """
    sql """ drop table if exists test_cast_to_datetime2; """
    sql """
        create table test_cast_to_datetime1 (k1 int, k2 date) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        create table test_cast_to_datetime2 (k1 int, k2 date) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """ insert into test_cast_to_datetime2 values(1, null); """
    sql """ set enable_insert_strict=true; """
    sql """
        insert into test_cast_to_datetime1 select k1, cast(cast(k2 as char) as date) from test_cast_to_datetime2;
    """
    qt_select_cast_date3 """ select * from  test_cast_to_datetime1; """

    sql """ drop table if exists test_cast_to_datetime1; """
    sql """ drop table if exists test_cast_to_datetime2; """
    sql """
        create table test_cast_to_datetime1 (k1 int, k2 datetime) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        create table test_cast_to_datetime2 (k1 int, k2 datetime) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """ insert into test_cast_to_datetime2 values(1, null); """
    sql """
        insert into test_cast_to_datetime1 select k1, cast(cast(k2 as char) as datetime) from test_cast_to_datetime2;
    """
    qt_select_cast_date4 """ select * from  test_cast_to_datetime1; """

    sql """ ADMIN SET FRONTEND CONFIG ("enable_date_conversion" = "true"); """
}