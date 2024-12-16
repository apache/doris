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
        DROP TABLE IF EXISTS `fn_test_bitmap_not_nullable`
    """ 
    sql """
        DROP TABLE IF EXISTS `fn_test_bitmap`
    """

    // test ipv4/ipv6
    sql """ drop table if exists fn_test_ip_nullable """
    sql """ CREATE TABLE IF NOT EXISTS fn_test_ip_nullable (id int, ip4 ipv4, ip6 ipv6, ip4_str string, ip6_str string) engine=olap
                                                                                         DISTRIBUTED BY HASH(`id`) BUCKETS 4
                                                                                         properties("replication_num" = "1") """

    sql """ drop table if exists fn_test_ip_not_nullable """
    sql """ CREATE TABLE IF NOT EXISTS fn_test_ip_not_nullable (id int, ip4 ipv4 not null, ip6 ipv6 not null, ip4_str string, ip6_str string) engine=olap
                                                                                         DISTRIBUTED BY HASH(`id`) BUCKETS 4
                                                                                         properties("replication_num" = "1") """

    // test ip with rowstore
    sql """ drop table if exists fn_test_ip_nullable_rowstore """
    sql """ CREATE TABLE IF NOT EXISTS fn_test_ip_nullable_rowstore (id int, ip4 ipv4, ip6 ipv6, ip4_str string, ip6_str string) engine=olap
                                                                                            UNIQUE KEY(`id`)
                                                                                         DISTRIBUTED BY HASH(`id`) BUCKETS 4
                                                                                         properties("replication_num" = "1", "store_row_column" = "true") """
    sql """ drop table if exists fn_test_ip_not_nullable_rowstore """
    sql """ CREATE TABLE IF NOT EXISTS fn_test_ip_not_nullable_rowstore (id int, ip4 ipv4 not null, ip6 ipv6 not null, ip4_str string, ip6_str string) engine=olap
                                                                                            UNIQUE KEY(`id`)
                                                                                         DISTRIBUTED BY HASH(`id`) BUCKETS 4
                                                                                         properties("replication_num" = "1", "store_row_column" = "true") """
    // make some special ip address
    /***
     回环地址
    1;127.0.0.1;::1
    // 私有地址
    - 网络地址 (最小地址)
    2;10.0.0.0;fc00::
    - 广播地址 (最大地址)
    3;10.255.255.255;fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff
    - 网络地址 (最小地址)
    4;172.16.0.0;fc00::
    - 广播地址 (最大地址)
    5;172.31.255.255;febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff
    - 网络地址 (最小地址)
    6;192.168.0.0;fe80::
    - 广播地址 (最大地址)
    7;192.168.255.255;ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff
    // 链路本地地址
    8;169.254.0.0;fe80::
    // 公有地址
    9;8.8.8.8;2001:4860:4860::8888  // Google Public DNS
    10;1.1.1.1;2606:4700:4700::1111  // Cloudflare DNS
    // 组播地址
    11;224.0.0.0;ff01::  // 所有主机
    12;239.255.255.255;ff02::1  // 所有路由器
    // 仅用于文档示例的地址
    13;192.0.2.0;2001:0db8:85a3::8a2e:0370:7334
    14;203.0.113.0;2001:db8::1
    15;198.51.100.0;2001:db8::2
    // 本地回环地址
    16;localhost;::1
    // IPv4 特殊地址
    17;240.0.0.0;null  // 保留地址
    18;255.255.255.255;null  // 广播地址
    // 唯一本地地址
    19;null;fd00::  // 唯一本地地址 (ULA)
    // A 类地址
    - 网络地址 (最小地址)
    20;0.0.0.0;null
    - 最大地址
    21;127.255.255.255;null
    // B 类地址
    - 网络地址 (最小地址)
    22;128.0.0.0;null
    - 最大地址
    23;191.255.255.255;null
    // C 类地址
    - 网络地址 (最小地址)
    24;192.0.0.0;null
    - 最大地址
    25;223.255.255.255;null
    // D 类地址
    - 组播地址 (最小地址)
    26;224.0.0.0;ff01::
    - 最大地址
    27;239.255.255.255;ff02::1
    // 无效的多播地址
    28;null;ff00::  // 保留地址
    ***/

    streamLoad {
        table "fn_test_ip_nullable"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_special.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(28, json.NumberTotalRows)
            assertEquals(28, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_nullable_rowstore"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_special.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(28, json.NumberTotalRows)
            assertEquals(28, json.NumberLoadedRows)
        }
    }

    // rowstore table to checkout with not rowstore table
    def sql_res = sql "select * from fn_test_ip_nullable order by id;"
    def sql_res_rowstore = sql "select * from fn_test_ip_nullable_rowstore order by id;"
    assertEquals(sql_res.size(), sql_res_rowstore.size())
    for (int i = 0; i < sql_res.size(); i++) {
        for (int j = 0; j < sql_res[i].size(); j++) {
            assertEquals(sql_res[i][j], sql_res_rowstore[i][j])
        }
    }

    if (!isClusterKeyEnabled()) {
    // test fn_test_ip_nullable_rowstore table with update action
    sql "update fn_test_ip_nullable_rowstore set ip4 = '' where id = 1;"
    sql_res = sql "select * from fn_test_ip_nullable_rowstore where id = 1;"
    log.info("sql_res: ${sql_res[0]}".toString())
    assertEquals(sql_res[0].toString(), '[1, null, ::1, "127.0.0.1", "::1"]')
    sql "update fn_test_ip_nullable_rowstore set ip6 = '' where id = 1;"
    sql_res = sql "select * from fn_test_ip_nullable_rowstore where id = 1;"
    assertEquals(sql_res[0].toString(), '[1, null, null, "127.0.0.1", "::1"]')
    sql "update fn_test_ip_nullable_rowstore set ip4 = '127.0.0.1' where id = 1;"
    sql_res = sql "select * from fn_test_ip_nullable_rowstore where id = 1;"
    assertEquals(sql_res[0].toString(), '[1, 127.0.0.1, null, "127.0.0.1", "::1"]')
    sql "update fn_test_ip_nullable_rowstore set ip6 = '::1' where id = 1;"
    sql_res = sql "select * from fn_test_ip_nullable_rowstore where id = 1;"
    assertEquals(sql_res[0].toString(), '[1, 127.0.0.1, ::1, "127.0.0.1", "::1"]')
    }

    streamLoad {
        table "fn_test_ip_not_nullable"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_special_no_null.csv"
        set 'column_separator', ';'
        set "max_filter_ratio", "0.1"
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(28, json.NumberTotalRows)
            assertEquals(27, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_not_nullable_rowstore"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_special_no_null.csv"
        set 'column_separator', ';'
        set "max_filter_ratio", "0.1"
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(28, json.NumberTotalRows)
            assertEquals(27, json.NumberLoadedRows)
        }
    }

    // rowstore table to checkout with not rowstore table
    def sql_res_not_null = sql "select * from fn_test_ip_not_nullable_rowstore order by id;"
    def sql_res_not_null_rowstore = sql "select * from fn_test_ip_not_nullable_rowstore order by id;"
    assertEquals(sql_res_not_null.size(), sql_res_not_null_rowstore.size())
    for (int i = 0; i < sql_res_not_null.size(); i++) {
        for (int j = 0; j < sql_res_not_null[i].size(); j++) {
            assertEquals(sql_res_not_null[i][j], sql_res_not_null_rowstore[i][j])
        }
    }

    if (!isClusterKeyEnabled()) {
    // test fn_test_ip_not_nullable_rowstore table with update action
    // not null will throw exception if we has data in table
    test {
        sql "update fn_test_ip_not_nullable_rowstore set ip4 = '' where id = 1;"
        exception("Insert has filtered data in strict mode")
    }

    test {
        sql "update fn_test_ip_not_nullable_rowstore set ip6 = '' where id = 1;"
        exception("Insert has filtered data in strict mode")
    }

    sql "update fn_test_ip_not_nullable_rowstore set ip4 = '192.10.10.1' where id = 1;"
    def sql_res1 = sql "select * from fn_test_ip_not_nullable_rowstore where id = 1;"
    log.info("sql_res: ${sql_res1[0]}".toString())
    assertEquals(sql_res1[0].toString(), '[1, 192.10.10.1, ::1, "127.0.0.1", "::1"]')
    sql "update fn_test_ip_not_nullable_rowstore set ip6 = '::2' where id = 1;"
    sql_res1 = sql "select * from fn_test_ip_not_nullable_rowstore where id = 1;"
    assertEquals(sql_res1[0].toString(), '[1, 192.10.10.1, ::2, "127.0.0.1", "::1"]')
    }

    // make some normal ipv4/ipv6 data for sql function , which is increased one by one
    // 29-50 A 类地址 ; 51-68 B 类地址 ; 69-87 C 类地址 ; 88-100 D 类地址
    streamLoad {
        table "fn_test_ip_nullable"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_normal.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(72, json.NumberTotalRows)
            assertEquals(72, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_nullable_rowstore"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_normal.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(72, json.NumberTotalRows)
            assertEquals(72, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_not_nullable"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_normal.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(72, json.NumberTotalRows)
            assertEquals(72, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_not_nullable_rowstore"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_normal.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(72, json.NumberTotalRows)
            assertEquals(72, json.NumberLoadedRows)
        }
    }

    streamLoad {
        table "fn_test_ip_not_nullable"
        db "regression_test_nereids_function_p0"
        file "fn_test_ip_invalid.csv"
        set 'column_separator', ';'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(31, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
        }
    }


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
            `polygon_wkt` varchar(50) null,
            `km_bool_tint` map<boolean, tinyint> null,
            `km_tint_tint` map<tinyint, tinyint> null,
            `km_sint_tint` map<smallint, tinyint> null,
            `km_int_tint` map<int, tinyint> null,
            `km_bint_tint` map<bigint, tinyint> null,
            `km_lint_tint` map<largeint, tinyint> null,
            `km_float_tint` map<float, tinyint> null,
            `km_dbl_tint` map<double, tinyint> null,
            `km_dcml_tint` map<decimal(22,9), tinyint> null,
            `km_chr_tint` map<char(5), tinyint> null,
            `km_vchr_tint` map<varchar(50), tinyint> null,
            `km_str_tint` map<string, tinyint> null,
            `km_date_tint` map<date, tinyint> null,
            `km_dtm_tint` map<datetime, tinyint> null,
            `km_tint_bool` map<tinyint, boolean> null,
            `km_int_int` map<int, int> null,
            `km_tint_sint` map<tinyint, smallint> null,
            `km_tint_int` map<tinyint, int> null,
            `km_tint_bint` map<tinyint, bigint> null,
            `km_tint_lint` map<tinyint, largeint> null,
            `km_tint_float` map<tinyint, float> null,
            `km_tint_dbl` map<tinyint, double> null,
            `km_tint_dcml` map<tinyint, decimal(22,9)> null,
            `km_tint_chr` map<tinyint, char(5)> null,
            `km_tint_vchr` map<tinyint, varchar(50)> null,
            `km_tint_str` map<tinyint, string> null,
            `km_tint_date` map<tinyint, date> null,
            `km_tint_dtm` map<tinyint, datetime> null,
            `kjson` JSON null,
            `kstruct` STRUCT<id: int> null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """

    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_bitmap` (
            `id` int null,
            `kbitmap` bitmap bitmap_union 
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
            `x_lng` double not null,
            `x_lat` double not null,
            `y_lng` double not null,
            `y_lat` double not null,
            `z_lng` double not null,
            `z_lat` double not null,
            `radius` double not null,
            `linestring_wkt` varchar(50) not null,
            `polygon_wkt` varchar(50) not null,
            `km_bool_tint` map<boolean, tinyint> not null,
            `km_tint_tint` map<tinyint, tinyint> not null,
            `km_sint_tint` map<smallint, tinyint> not null,
            `km_int_tint` map<int, tinyint> not null,
            `km_bint_tint` map<bigint, tinyint> not null,
            `km_lint_tint` map<largeint, tinyint> not null,
            `km_float_tint` map<float, tinyint> not null,
            `km_dbl_tint` map<double, tinyint> not null,
            `km_dcml_tint` map<decimal(22,9), tinyint> not null,
            `km_chr_tint` map<char(5), tinyint> not null,
            `km_vchr_tint` map<varchar(50), tinyint> not null,
            `km_str_tint` map<string, tinyint> not null,
            `km_date_tint` map<date, tinyint> not null,
            `km_dtm_tint` map<datetime, tinyint> not null,
            `km_tint_bool` map<tinyint, boolean> not null,
            `km_int_int` map<int, int> not null,
            `km_tint_sint` map<tinyint, smallint> not null,
            `km_tint_int` map<tinyint, int> not null,
            `km_tint_bint` map<tinyint, bigint> not null,
            `km_tint_lint` map<tinyint, largeint> not null,
            `km_tint_float` map<tinyint, float> not null,
            `km_tint_dbl` map<tinyint, double> not null,
            `km_tint_dcml` map<tinyint, decimal(22,9)> not null,
            `km_tint_chr` map<tinyint, char(5)> not null,
            `km_tint_vchr` map<tinyint, varchar(50)> not null,
            `km_tint_str` map<tinyint, string> not null,
            `km_tint_date` map<tinyint, date> not null,
            `km_tint_dtm` map<tinyint, datetime> not null,
            `kjson` JSON not null,
            `kstruct` STRUCT<id: int> not null
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        properties("replication_num" = "1")
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_bitmap_not_nullable` (
            `id` int not null,
            `kbitmap` bitmap bitmap_union not null
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
            st_point_str, st_point_vc, x_lng, x_lat, y_lng, y_lat, z_lng, z_lat, radius, linestring_wkt, polygon_wkt,
            km_bool_tint, km_tint_tint, km_sint_tint, km_int_tint, km_bint_tint, km_lint_tint, km_float_tint,
            km_dbl_tint, km_dcml_tint, km_chr_tint, km_vchr_tint, km_str_tint, km_date_tint, km_dtm_tint,
            km_tint_bool, km_int_int, km_tint_sint, km_tint_int, km_tint_bint, km_tint_lint, km_tint_float,
            km_tint_dbl, km_tint_dcml, km_tint_chr, km_tint_vchr, km_tint_str, km_tint_date, km_tint_dtm, kjson, kstruct
            '''
        file "fn_test.dat"
    }

    streamLoad {
        table "fn_test_bitmap"
        db "regression_test_nereids_function_p0"
        set 'column_separator', ';'
        set 'columns', '''
            id, kbitmap=to_bitmap(id)
            '''
        file "fn_test_bitmap.dat"
    }

    sql """
        insert into fn_test_not_nullable select * from fn_test where id is not null
    """
    sql """
        insert into fn_test_bitmap_not_nullable select * from fn_test_bitmap where id is not null
    """

    sql """ set enable_decimal256 = true """
    sql """ drop table if exists fn_test_array_with_large_decimal """
    sql """
    create table IF NOT EXISTS fn_test_array_with_large_decimal(id int, a array<tinyint>, b array<decimal(10,0)>, c array<decimal(76,56)>) properties('replication_num' = '1');
    """
    streamLoad {
        table "fn_test_array_with_large_decimal"
        db "regression_test_nereids_function_p0"
        set 'column_separator', ';'
        file "test_array_large_decimal.csv"
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(100, json.NumberTotalRows)
            assertEquals(100, json.NumberLoadedRows)
            }
     }
	// array_match_any && array_match_all
	sql """ drop table if exists fn_test_am """
	sql """ CREATE TABLE IF NOT EXISTS fn_test_am (id int, kastr array<string>, kaint array<int>) engine=olap
                                                                                         DISTRIBUTED BY HASH(`id`) BUCKETS 4
                                                                                         properties("replication_num" = "1") """
    streamLoad {
        table "fn_test_am"
        db "regression_test_nereids_function_p0"
        file "fn_test_am.csv"
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(102, json.NumberTotalRows)
            assertEquals(102, json.NumberLoadedRows)
        }
    }

}
