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

suite("nereids_function") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    // ddl begin
    sql "drop table if exists t1"
    sql "drop table if exists t2"
    sql "drop table if exists t3"

    sql """
        CREATE TABLE IF NOT EXISTS `t1` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """
    sql """
        CREATE TABLE IF NOT EXISTS `t2` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace_if_not_null null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """
    sql """
        CREATE TABLE IF NOT EXISTS `t3` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        properties("replication_num" = "1")
    """

    sql """
    insert into t1 values
    (0, 1, 1989, 1001, 11011902, 123.123, true, 1989-03-21, 1989-03-21 13:00:00, wangjuoo4, 0.1, 6.333, string12345, 170141183460469231731687303715884105727),
    (0, 2, 1986, 1001, 11011903, 1243.5, false, 1901-12-31, 1989-03-21 13:00:00, wangynnsf, 20.268, 789.25, string12345, -170141183460469231731687303715884105727),
    (0, 3, 1989, 1002, 11011905, 24453.325, false, 2012-03-14, 2000-01-01 00:00:00, yunlj8@nk, 78945, 3654.0, string12345, 0),
    (0, 4, 1991, 3021, -11011907, 243243.325, false, 3124-10-10, 2015-03-13 10:30:00, yanvjldjlll, 2.06, -0.001, string12345, 20220101),
    (0, 5, 1985, 5014, -11011903, 243.325, true, 2015-01-01, 2015-03-13 12:36:38, du3lnvl, -0.000, -365, string12345, 20220102),
    (0, 6, 32767, 3021, 123456, 604587.000, true, 2014-11-11, 2015-03-13 12:36:38, yanavnd, 0.1, 80699, string12345, 20220104),
    (0, 7, -32767, 1002, 7210457, 3.141, false, 1988-03-21, 1901-01-01 00:00:00, jiw3n4, 0.0, 6058, string12345, -20220101),
    (1, 8, 255, 2147483647, 11011920, -0.123, true, 1989-03-21, 9999-11-11 12:12:00, wangjuoo5, 987456.123, 12.14, string12345, -2022),
    (1, 9, 1991, -2147483647, 11011902, -654.654, true, 1991-08-11, 1989-03-21 13:11:00, wangjuoo4, 0.000, 69.123, string12345, 11011903),
    (1, 10, 1991, 5014, 9223372036854775807, -258.369, false, 2015-04-02, 2013-04-02 15:16:52, wangynnsf, -123456.54, 0.235, string12345, -11011903),
    (1, 11, 1989, 25699, -9223372036854775807, 0.666, true, 2015-04-02, 1989-03-21 13:11:00, yunlj8@nk, -987.001, 4.336, string12345, 1701411834604692317316873037158),
    (1, 12, 32767, -2147483647, 9223372036854775807, 243.325, false, 1991-08-11, 2013-04-02 15:16:52, lifsno, -564.898, 3.141592654, string12345, 1701604692317316873037158),
    (1, 13, -32767, 2147483647, -9223372036854775807, 100.001, false, 2015-04-02, 2015-04-02 00:00:00, wenlsfnl, 123.456, 3.141592653, string12345, 701411834604692317316873037158),
    (1, 14, 255, 103, 11011902, -0.000, false, 2015-04-02, 2015-04-02 00:00:00,  , 3.141592654, 2.036, string12345, 701411834604692317316873),
    (1, 15, 1992, 3021, 11011920, 0.00, true, 9999-12-12, 2015-04-02 00:00:00, , 3.141592653, 20.456, string12345, 701411834604692317),
    (null, null, null, null, null, null, null, null, null, null, null, null, null, null)
    """
    sql "insert into t2 select * from t1 where k1 <= 3"
    sql "insert into t3 select * from t1"
    // ddl end
    
    // function table begin
    // usage: write the new function in the list and construct answer.
    def scalar_function = [
        "abs":[["int", "int"], ["double", "double"]],
        
    ]
    // function table end

    // test begin
    // test end
}