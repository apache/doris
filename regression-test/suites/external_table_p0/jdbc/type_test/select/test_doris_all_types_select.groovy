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

suite("test_doris_all_types_select", "p0,external,doris,external_docker,external_docker_doris") {
    String jdbcUrl = context.config.jdbcUrl + "&sessionVariables=return_object_data_as_binary=true"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    String doris_port = context.config.otherConfigs.get("doris_port");

    sql """create database if not exists test_doris_all_types_select;"""

    sql """use internal.test_doris_all_types_select;"""

    sql """drop table if exists all_types_nullable"""
    sql """
        create table all_types_nullable (
            bool_col boolean,
            tinyint_col tinyint,
            smallint_col smallint,
            int_col int,
            bigint_col bigint,
            largeint_col largeint,
            float_col float,
            double_col double,
            decimal_col decimal(38, 10),
            date_col date,
            datetime_col1 datetime,
            datetime_col2 datetime(3),
            datetime_col3 datetime(6),
            char_col char(255),
            varchar_col varchar(65533),
            string_col string,
            json_col json,
        )
        DUPLICATE KEY(`bool_col`)
        DISTRIBUTED BY HASH(`bool_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into all_types_nullable values (false, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105727, -3.4028234e+38, -1.7976931348623157e+308, -9999999999999999999999999999.9999999999, '0000-01-01', '0000-01-01 00:00:00', '0000-01-01 00:00:00.000', '0000-01-01 00:00:00.000000', 'a', 'a', '-string', '{\"a\": 0}');"""
    sql """insert into all_types_nullable values (true, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 3.4028234e+38, 1.7976931348623157e+308, 9999999999999999999999999999.9999999999, '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999', '9999-12-31 23:59:59.999999', REPEAT('a', 255), REPEAT('a', 65533), '+string', '{\"a\": 1}');"""
    sql """insert into all_types_nullable values (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""

    sql """drop table if exists all_types_non_nullable"""
    sql """
        create table all_types_non_nullable (
            bool_col boolean not null,
            tinyint_col tinyint not null,
            smallint_col smallint not null,
            int_col int not null,
            bigint_col bigint not null,
            largeint_col largeint not null,
            float_col float not null,
            double_col double not null,
            decimal_col decimal(38, 10) not null,
            date_col date not null,
            datetime_col1 datetime not null,
            datetime_col2 datetime(3) not null,
            datetime_col3 datetime(6) not null,
            char_col char(255) not null,
            varchar_col varchar(65533) not null,
            string_col string not null,
            json_col json not null,
        )
        DUPLICATE KEY(`bool_col`)
        DISTRIBUTED BY HASH(`bool_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into all_types_non_nullable values (false, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105727, -3.4028234e+38, -1.7976931348623157e+308, -9999999999999999999999999999.9999999999, '0000-01-01', '0000-01-01 00:00:00', '0000-01-01 00:00:00.000', '0000-01-01 00:00:00.000000', 'a', 'a', '-string', '{\"a\": 0}');"""
    sql """insert into all_types_non_nullable values (true, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 3.4028234e+38, 1.7976931348623157e+308, 9999999999999999999999999999.9999999999, '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999', '9999-12-31 23:59:59.999999', REPEAT('a', 255), REPEAT('a', 65533), '+string', '{\"a\": 1}');"""

    sql """drop table if exists t_hll_bitmap"""

    sql """
        create table t_hll_bitmap (
            k int,
            bitmap_c bitmap BITMAP_UNION,
            hll_c hll HLL_UNION
        )
        aggregate key (k)
        distributed by hash(k) buckets 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """insert into t_hll_bitmap values (1, to_bitmap(1), hll_hash(1));"""
    sql """insert into t_hll_bitmap values (2, to_bitmap(2), hll_hash(2));"""
    sql """insert into t_hll_bitmap values (3, to_bitmap(3), hll_hash(3));"""
    sql """insert into t_hll_bitmap values (4, to_bitmap(4), hll_hash(4));"""

    sql """drop table if exists arr_types_test """
    sql """
        create table arr_types_test (
            int_col int,
            arr_bool_col array<boolean>,
            arr_tinyint_col array<tinyint>,
            arr_smallint_col array<smallint>,
            arr_int_col array<int>,
            arr_bigint_col array<bigint>,
            arr_largeint_col array<largeint>,
            arr_float_col array<float>,
            arr_double_col array<double>,
            arr_decimal1_col array<decimal(10, 5)>,
            arr_decimal2_col array<decimal(30, 10)>,
            arr_date_col array<date>,
            arr_datetime_col array<datetime(3)>,
            arr_char_col array<char(10)>,
            arr_varchar_col array<varchar(10)>,
            arr_string_col array<string>
        )
        DUPLICATE KEY(`int_col`)
        DISTRIBUTED BY HASH(`int_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into arr_types_test values (1, array(true), array(1), array(1), array(1), array(1), array(1), array(1.0), array(1.0), array(1.0), array(1.0), array('2021-01-01'), array('2021-01-01 00:00:00.000'), array('a'), array('a'), array('a'));"""
    sql """insert into arr_types_test values (2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""

    sql """drop table if exists arr_nesting_types_test """
    sql """
        create table arr_nesting_types_test (
            int_col int,
            arr_arr_bool_col array<array<boolean>>,
            arr_arr_tinyint_col array<array<tinyint>>,
            arr_arr_smallint_col array<array<smallint>>,
            arr_arr_int_col array<array<int>>,
            arr_arr_bigint_col array<array<bigint>>,
            arr_arr_largeint_col array<array<largeint>>,
            arr_arr_float_col array<array<float>>,
            arr_arr_double_col array<array<double>>,
            arr_arr_decimal1_col array<array<decimal(10, 5)>>,
            arr_arr_decimal2_col array<array<decimal(30, 10)>>,
            arr_arr_date_col array<array<date>>,
            arr_arr_datetime_col array<array<datetime(3)>>,
            arr_arr_char_col array<array<char(10)>>,
            arr_arr_varchar_col array<array<varchar(10)>>,
            arr_arr_string_col array<array<string>>
        )
        DUPLICATE KEY(`int_col`)
        DISTRIBUTED BY HASH(`int_col`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into arr_nesting_types_test values (1, array(array(true)), array(array(1)), array(array(1)), array(array(1)), array(array(1)), array(array(1)), array(array(1.0)), array(array(1.0)), array(array(1.0)), array(array(1.0)), array(array('2021-01-01')), array(array('2021-01-01 00:00:00.000')), array(array('a')), array(array('a')), array(array('a')));"""
    sql """insert into arr_nesting_types_test values (2, array(array(true),array(true)), array(array(1),array(1)), array(array(1),array(1)), array(array(1),array(1)), array(array(1),array(1)), array(array(1),array(1)), array(array(1.0),array(1.0)), array(array(1.0),array(1.0)), array(array(1.0),array(1.0)), array(array(1.0),array(1.0)), array(array('2021-01-01'),array('2021-01-01')), array(array('2021-01-01 00:00:00.000'),array('2021-01-01 00:00:00.000')), array(array('a'),array('a')), array(array('a'),array('a')), array(array('a'),array('a')));"""
    sql """insert into arr_nesting_types_test values (3, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);"""

    sql """drop catalog if exists doris_all_type_test """

    sql """ CREATE CATALOG doris_all_type_test PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )
    """

    qt_desc_all_types_nullable """desc doris_all_type_test.test_doris_all_types_select.all_types_nullable;"""
    qt_select_all_types_nullable """select * except(float_col) from doris_all_type_test.test_doris_all_types_select.all_types_nullable order by 1;"""

    qt_desc_all_types_non_nullable """desc doris_all_type_test.test_doris_all_types_select.all_types_non_nullable;"""
    qt_select_all_types_non_nullable """select * except(float_col) from doris_all_type_test.test_doris_all_types_select.all_types_non_nullable order by 1;"""

    qt_desc_bitmap_hll """desc doris_all_type_test.test_doris_all_types_select.t_hll_bitmap;"""
    qt_select_bitmap_hll """select k, bitmap_union_count(bitmap_c), hll_union_agg(hll_c) from doris_all_type_test.test_doris_all_types_select.t_hll_bitmap group by k order by 1;"""

    qt_desc_arr_types_test """desc doris_all_type_test.test_doris_all_types_select.arr_types_test;"""
    qt_select_arr_types_test """select * from doris_all_type_test.test_doris_all_types_select.arr_types_test order by 1;"""

    qt_desc_arr_nesting_types_test """desc doris_all_type_test.test_doris_all_types_select.arr_nesting_types_test;"""
    qt_select_arr_nesting_types_test """select * from doris_all_type_test.test_doris_all_types_select.arr_nesting_types_test order by 1;"""

    sql """drop database if exists test_doris_all_types_select;"""
    sql """drop catalog if exists doris_all_type_test """

}
