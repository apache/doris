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
    sql "drop table if exists fn_test_not_nullable"

    sql """
        CREATE TABLE IF NOT EXISTS `fn_test` (
            `id` int null,
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
            `kchrs1` char(10) null,
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
            `katint` array<tinyint(4)> null,
            `kasint` array<smallint(6)> null,
            `kaint` array<int> null,
            `kabint` array<bigint(20)> null,
            `kalint` array<largeint(40)> null,
            `kafloat` array<float> null,
            `kadbl` array<double> null,
            `kadt` array<date> null,
            `kadtm` array<datetime> null,
            `kadtv2` array<datev2> null,
            `kadtmv2` array<datetimev2(6)> null,
            `kachr` array<char(50)> null,
            `kavchr` array<varchar(50)> null,
            `kastr` array<string> null,
            `kadcml` array<decimal(27, 9)> null,
            `st_point_str` string null,
            `st_point_vc` varchar(50) null,
            `x_lng` double null,
            `x_lat` double null,
            `y_lng` double null,
            `y_lat` double null,
            `z_lng` double null,
            `z_lat` double null,
            `radius` double null,
            `linestring_wkt` varchar(50) null,
            `polygon_wkt` varchar(50) null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_not_nullable` (
            `id` int not null,
            `kbool` boolean not null,
            `ktint` tinyint(4) not null,
            `ksint` smallint(6) not null,
            `kint` int(11) not null,
            `kbint` bigint(20) not null,
            `klint` largeint(40) not null,
            `kfloat` float not null,
            `kdbl` double not null,
            `kdcmls1` decimal(9, 3) not null,
            `kdcmls2` decimal(15, 5) not null,
            `kdcmls3` decimal(27, 9) not null,
            `kdcmlv3s1` decimalv3(9, 3) not null,
            `kdcmlv3s2` decimalv3(15, 5) not null,
            `kdcmlv3s3` decimalv3(27, 9) not null,
            `kchrs1` char(10) not null,
            `kchrs2` char(20) not null,
            `kchrs3` char(50) not null,
            `kvchrs1` varchar(10) not null,
            `kvchrs2` varchar(20) not null,
            `kvchrs3` varchar(50) not null,
            `kstr` string not null,
            `kdt` date not null,
            `kdtv2` datev2 not null,
            `kdtm` datetime not null,
            `kdtmv2s1` datetimev2(0) not null,
            `kdtmv2s2` datetimev2(4) not null,
            `kdtmv2s3` datetimev2(6) not null,
            `kabool` array<boolean> not null,
            `katint` array<tinyint(4)> not null,
            `kasint` array<smallint(6)> not null,
            `kaint` array<int> not null,
            `kabint` array<bigint(20)> not null,
            `kalint` array<largeint(40)> not null,
            `kafloat` array<float> not null,
            `kadbl` array<double> not null,
            `kadt` array<date> not null,
            `kadtm` array<datetime> not null,
            `kadtv2` array<datev2> not null,
            `kadtmv2` array<datetimev2(6)> not null,
            `kachr` array<char(50)> not null,
            `kavchr` array<varchar(50)> not null,
            `kastr` array<string> not null,
            `kadcml` array<decimal(27, 9)> not null,
            `st_point_str` string not null,
            `st_point_vc` varchar(50) not null,
            `x_lng` double null,
            `x_lat` double null,
            `y_lng` double null,
            `y_lat` double null,
            `z_lng` double null,
            `z_lat` double null,
            `radius` double null,
            `linestring_wkt` varchar(50) null,
            `polygon_wkt` varchar(50) null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """
    // ddl end

    streamLoad {
        table "fn_test"
        db "regression_test_nereids_function_p0"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbool, ktint, ksint, kint, kbint, klint, kfloat, kdbl, kdcmls1, kdcmls2, kdcmls3,
            kdcmlv3s1, kdcmlv3s2, kdcmlv3s3, kchrs1, kchrs2, kchrs3, kvchrs1, kvchrs2, kvchrs3, kstr,
            kdt, kdtv2, kdtm, kdtmv2s1, kdtmv2s2, kdtmv2s3, kabool, katint, kasint, kaint,
            kabint, kalint, kafloat, kadbl, kadt, kadtm, kadtv2, kadtmv2, kachr, kavchr, kastr, kadcml,
            st_point_str, st_point_vc, x_lng, x_lat, y_lng, y_lat, z_lng, z_lat, radius, linestring_wkt, polygon_wkt
            '''
        file "fn_test.dat"
    }

    sql """
        insert into fn_test_not_nullable select * from fn_test where id is not null
    """
}