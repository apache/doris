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

import java.text.SimpleDateFormat

suite("test_cast_str_date_fail") {

    sql """ DROP TABLE IF EXISTS aec_46902; """
    sql """
     CREATE TABLE IF NOT EXISTS aec_46902(
            `ID` bigint NOT NULL,
            `CD` date NOT NULL,
            `PI` varchar(160) NOT NULL
            )
            DISTRIBUTED BY HASH(`ID`) BUCKETS 2
            properties("replication_num" = "1");
    """
    
    sql """ insert into aec_46902 values 
    (1, "2028-01-01", "2028---___%%%01-__01dfsdTfaTTTTT01:01"),
    (2, "2028-01-01", "2028-01-01__wer1q23rads"),
    (3, "2028-01-01", "2028__01-01dfsdTfaTTTTT01:01"),
    (4, "2028-01-01", "2028-01a01 T  10:10:10.234"),
    (5, "2028-01-01", "2028-01-01_"),
    (6, "2028-01-01", "2028-01-01}"),
    (7, "2028-01-01", "2028-"),
    (8, "2028-01-01", "2028-01-"),
    (9, "2028-01-01", "2028-01-01T"),
    (10, "2028-01-01", "2028-01-01T10:"),
    (11, "2028-01-01", "2028-01-01T10:10:"),
    (12, "2028-01-01", "2028-01-01T10:10:10."),
    (13, "2028-01-01", "2028"),
    (14, "2028-01-01", "2028-01"),
    (15, "2028-01-01", "2028-01-01"),
    (16, "2028-01-01", "2028-01-01 T  10"),
    (17, "2028-01-01", "2028-01-01 T  10:10"),
    (18, "2028-01-01", "2028-01-01 T  10:10:10"),
    (19, "2028-01-01", "2028-01-01 T  10:10:10.234"),
    (20, "2028-01-01", "2028-01-01 T  10:10:10.234a");
    """

    qt_cast_fail1 "select ID, PI, cast(trim(PI) AS datetimev2) from aec_46902 where CD = trim('2028-01-01') order by ID"
    qt_cast_fail2 "select ID, PI, cast(trim(PI) AS datetimev2), PI=CD from aec_46902 where CD = trim('2028-01-01') and PI=CD order by ID"

    sql """ DROP TABLE IF EXISTS aec_46902; """
}
