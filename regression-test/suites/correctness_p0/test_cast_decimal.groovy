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

suite("test_cast_decimal") {
    explain {
        sql """select cast(32123.34212456734 as decimal(3,2));"""
        contains "CAST(32123.34212456734 AS decimalv3(3,2))"
    }

    sql """drop table if exists test_ttt"""
    sql """create table test_ttt(big_key bigint)DISTRIBUTED BY HASH(big_key) BUCKETS 1 PROPERTIES ("replication_num" = "1");"""
    sql """set enable_nereids_planner=false;"""
    sql """set enable_fold_constant_by_be = false; """
    sql """SELECT 1
            FROM test_ttt e1
            HAVING truncate(100, 2) < -2308.57
            AND cast(round(round(465.56, min(-5.987)), 2) AS DECIMAL) in
            (SELECT cast(truncate(round(8990.65 - 4556.2354, 2.4652), 2)AS DECIMAL)
            FROM test_ttt r2);"""
}
