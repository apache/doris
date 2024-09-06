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
    sql "drop table if exists test_timezone"
    sql """
    CREATE TABLE IF NOT EXISTS `test_timezone` (
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
    if (isGroupCommitMode()) {
        sql """ set enable_nereids_planner = true """
    }
    sql """insert into test_timezone values('2022-01-01 01:02:55', '2022-01-01 01:02:55.123')"""
    sql """insert into test_timezone values('2022-02-01 01:02:55Z', '2022-02-01 01:02:55.123Z')"""
    sql """insert into test_timezone values('2022-03-01 01:02:55+08:00', '2022-03-01 01:02:55.123UTC')"""
    sql """insert into test_timezone values('2022-04-01T01:02:55-06:00', '2022-04-01T01:02:55.123+06:00')"""
    sql """insert into test_timezone values('2022-05-01 01:02:55+02:30', '2022-05-01 01:02:55.123-02:30')"""
    sql """insert into test_timezone values('2022-06-01T01:02:55+04:30', '2022-06-01 01:02:55.123-07:30')"""
    sql """insert into test_timezone values('20220701010255+07:00', '20220701010255-05:00')"""
    if (isGroupCommitMode()) {
        sql """insert into test_timezone values('2022-07-31 21:00', '2022-08-01')"""
    } else {
        sql """insert into test_timezone values('20220801+05:00', '20220801America/Argentina/Buenos_Aires')"""
    }
    qt_legacy "select * from test_timezone order by k1"

    sql """ truncate table test_timezone """
    
    sql """ set enable_nereids_planner = true """
    sql """insert into test_timezone values('2022-01-01 01:02:55', '2022-01-01 01:02:55.123')"""
    sql """insert into test_timezone values('2022-02-01 01:02:55Z', '2022-02-01 01:02:55.123Z')"""
    sql """insert into test_timezone values('2022-05-01 01:02:55+02:30', '2022-05-01 01:02:55.123-02:30')"""
    sql """insert into test_timezone values('2022-06-01T01:02:55+04:30', '2022-06-01 01:02:55.123-07:30')"""
    sql """insert into test_timezone values('20220701010255+07:00', '20220701010255-05:00')"""
    qt_nereids "select * from test_timezone order by k1"

    qt_fold1 """ select cast('2020-12-12T12:12:12asia/shanghai' as datetime); """
    qt_fold2 """ select cast('2020-12-12T12:12:12america/los_angeLES' as datetime); """
    qt_fold3 """ select cast('2020-12-12T12:12:12Europe/pARIS' as datetime); """
}
