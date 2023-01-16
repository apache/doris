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

suite("load") {

    // ddl begin
    sql "drop table if exists fn_test"

    sql """
        CREATE TABLE IF NOT EXISTS `fn_test` (
            `kbool` boolean null,
            `ktint` tinyint(4) null,
            `ksint` smallint(6) null,
            `kint` int(11) null,
            `kbint` bigint(20) null,
            `klint` largeint(40) null,
            `kfloat` float null,
            `kdbl` double null,
            `kdcmls1` decimal(9, 3) null,
            `kdcmls2` decimal(15, 5) null,
            `kdcmls3` decimal(27, 9) null,
            `kdcmlv3s1` decimalv3(9, 3) null,
            `kdcmlv3s2` decimalv3(15, 5) null,
            `kdcmlv3s3` decimalv3(27, 9) null,
            `kchrs1` char(5) null,
            `kchrs2` char(20) null,
            `kchrs3` char(50) null,
            `kvchrs1` varchar(10) null,
            `kvchrs2` varchar(20) null,
            `kvchrs3` varchar(50) null,
            `kstr` string null,
            `kdt` date null,
            `kdtv2` datev2 null,
            `kdtm` datetime null,
            `kdtmv2s1` datetimev2(0) null,
            `kdtmv2s2` datetimev2(4) null,
            `kdtmv2s3` datetimev2(6) null,
            `kabool` array<boolean> null,
            `katint` array<tinyint> null,
            `kasint` array<smallint> null,
            `kaint` array<int> null,
            `kabint` array<bigint> null,
            `kalint` array<largeint> null,
            `kafloat` array<float> null,
            `kadbl` array<double> null,
            `kadt` array<date> null,
            `kadtm` array<datetime> null,
            `kadtv2` array<datev2> null,
            `kadtmv2` array<datetimev2> null,
            `kachr` array<char> null,
            `kavchr` array<varchar> null,
            `kastr` array<string> null,
            `kadcml` array<decimal> null
        ) engine=olap
        DISTRIBUTED BY HASH(`ktint`) BUCKETS 5
        properties("replication_num" = "1")
    """
    // ddl end

    streamLoad {
        table "fn_test"
        db "regression_test_nereids_function_p0"
        set 'column_separator', ';'
        file "fn_test.dat"
    }
}