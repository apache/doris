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

suite("test_date_trunc") {
    
    def dbName = "test_date_trunc"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE $dbName"

    sql """
        CREATE TABLE IF NOT EXISTS `baseall` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    streamLoad {
        table "baseall"
        db dbName
        set 'column_separator', ','
        file "../../baseall.txt"
    }

    sql "sync"

    qt_select_date_trunc_second """ SELECT k11, date_trunc(k11,'SECoND') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_minute """ SELECT k11, date_trunc(k11,'MINutE') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_hour """ SELECT k11, date_trunc(k11,'Hour') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_day """ SELECT k11, date_trunc(k11,'DAY') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_week """ SELECT k11, date_trunc(k11,'Week') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_month """ SELECT k11, date_trunc(k11,'MONTH') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_quarter """ SELECT k11, date_trunc(k11,'quarter') FROM baseall order by k1,k2,k3;"""
    qt_select_date_trunc_year """ SELECT k11, date_trunc(k11,'YeaR') FROM baseall order by k1,k2,k3;"""

    qt_date_week """ SELECT k10, date_trunc(k10,'Week') FROM baseall order by k1,k2,k3;"""
    qt_date_year """ SELECT k10, date_trunc(k10,'YeaR') FROM baseall order by k1,k2,k3;"""

    try {
        sql """ SELECT date_trunc(k11,k7) FROM baseall ; """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("must be a string constant"), e.getMessage())
    }
    try {
        sql """ SELECT date_trunc(k11,'AAAA') FROM baseall ; """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("param only support argument"), e.getMessage())
    }

    try {
        sql """ SELECT date_trunc(k10,k7) FROM baseall ; """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("must be a string constant"), e.getMessage())
    }
    try {
        sql """ SELECT date_trunc(k10,'yearr') FROM baseall ; """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("param only support argument"), e.getMessage())
    }
}
