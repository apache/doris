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

suite("test_date_implicit_cast") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    def tbl = "test_date_implicit_cast"
    def result = ""
    def contain0 = false
    def contain1 = false

    sql """ DROP TABLE IF EXISTS `d4nn` """
    sql """ CREATE TABLE IF NOT EXISTS `d4nn` (
                `k1` DATETIMEV2(4) NOT NULL
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    result = sql " desc verbose select if(k1='2020-12-12', k1, '2020-12-12 12:12:12.123') from d4nn "
    for (String value : result) {
        if (value.contains("col=k1, colUniqueId=0, type=DATETIMEV2(4)")) {
            contain0 = true;
        }
        if (value.contains("col=null, colUniqueId=null, type=DATETIMEV2(4)")) {
            contain1 = true;
        }
    }
    assertTrue(contain0 && contain1, "failed on 1")
    

    result = sql " desc verbose select if(k1='2020-12-12', k1, cast('2020-12-12 12:12:12.123' as datetimev2(3))) from d4nn; "
    for (String value : result) {
        if (value.contains("col=k1, colUniqueId=0, type=DATETIMEV2(4)")) {
            contain0 = true;
        }
        if (value.contains("col=null, colUniqueId=null, type=DATETIMEV2(4)")) {
            contain1 = true;
        }
    }
    assertTrue(contain0 && contain1, "failed on 2")

    result = sql " desc verbose select if(k1='2012-12-12 12:12:12.1235', k1, '2020-12-12 12:12:12.12345') from d4nn; "
    for (String value : result) {
        if (value.contains("col=k1, colUniqueId=0, type=DATETIMEV2(4)")) {
            contain0 = true;
        }
        if (value.contains("col=null, colUniqueId=null, type=DATETIMEV2(5)")) {
            contain1 = true;
        }
    }
    assertTrue(contain0 && contain1, "failed on 3")
    

    sql """ DROP TABLE IF EXISTS `d6` """
    sql """ CREATE TABLE IF NOT EXISTS `d6` (
                `k1` DATETIMEV2(6) NULL
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    result = sql " desc verbose select if(k1='2020-12-12 12:12:12.12345', k1, '2020-12-12 12:12:12.33333') from d6; "
    for (String value : result) {
        if (value.contains("col=k1, colUniqueId=0, type=DATETIMEV2(6)")) {
            contain0 = true;
        }
        if (value.contains("col=null, colUniqueId=null, type=DATETIMEV2(6)")) {
            contain1 = true;
        }
    }
    assertTrue(contain0 && contain1, "failed on 4")
}