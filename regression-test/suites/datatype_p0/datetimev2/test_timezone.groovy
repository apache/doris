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

suite("test_timezone") {
    def table = "test_timezone"

    sql "drop table if exists ${table}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table}` (
      `k1` datetimev2(3) NOT NULL,
      `k2` datev2 NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_num" = "1"
    )
    """

    sql """ set time_zone = '+02:00' """

    sql """ set enable_nereids_planner = false """
    sql """insert into ${table} values('2022-01-01 01:02:55', '2022-01-01 01:02:55.123')"""
    sql """insert into ${table} values('2022-02-01 01:02:55Z', '2022-02-01 01:02:55.123Z')"""
    sql """insert into ${table} values('2022-03-01 01:02:55UTC+8', '2022-03-01 01:02:55.123UTC')"""
    sql """insert into ${table} values('2022-04-01T01:02:55UTC-6', '2022-04-01T01:02:55.123UTC+6')"""
    sql """insert into ${table} values('2022-05-01 01:02:55+02:30', '2022-05-01 01:02:55.123-02:30')"""
    sql """insert into ${table} values('2022-06-01T01:02:55+04:30', '2022-06-01 01:02:55.123-07:30')"""
    sql """insert into ${table} values('20220701010255+07:00', '20220701010255-05:00')"""
    sql """insert into ${table} values('20220801GMT+5', '20220801GMT-3')"""
    qt_analysis "select * from ${table} order by k1"

    sql """ truncate table ${table} """
    
    sql """ set enable_nereids_planner = true """
    sql """insert into ${table} values('2022-01-01 01:02:55', '2022-01-01 01:02:55.123')"""
    sql """insert into ${table} values('2022-02-01 01:02:55Z', '2022-02-01 01:02:55.123Z')"""
    sql """ set enable_nereids_planner = false """ // TODO remove it after nereids support this format
    sql """insert into ${table} values('2022-05-01 01:02:55+02:30', '2022-05-01 01:02:55.123-02:30')"""
    sql """insert into ${table} values('2022-06-01T01:02:55+04:30', '2022-06-01 01:02:55.123-07:30')"""
    sql """insert into ${table} values('20220701010255+07:00', '20220701010255-05:00')"""
    sql """ set enable_nereids_planner = true """
    qt_nereids "select * from ${table} order by k1"
}
