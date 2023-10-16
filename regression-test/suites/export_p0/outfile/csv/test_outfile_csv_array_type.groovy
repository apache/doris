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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_csv_array_type", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "outfile_csv_array_export_test"
    def load_table_name = "outfile_csv_array_type_load_test"
    def outFilePath = "${bucket}/outfile/csv/complex_type/exp_"


    def create_table = {table_name, struct_field ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `name` STRING COMMENT "用户年龄",
            ${struct_field}
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_S3 = {
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS csv
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }


    // 1. test NULL ARRAY
    try {
        def struct_field_define = "`a_info` ARRAY<int> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [9, 99, 999]), (2, 'doris2', [8, 88]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [1, null, 2]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, 1, 2, 999999, 111111]); """
        sql """ insert into ${export_table_name} values (8, 'doris8', null); """


        // test base data
        qt_select_base1 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load1 """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 2. test NOT NULL ARRAY
    try {
        def struct_field_define = "`a_info` ARRAY<int> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [9, 99, 999]), (2, 'doris2', [8, 88]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [1, null, 2]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, 1, 2, 999999, 111111]); """

        // test base data
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load2 """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 3. test NULL ARRAY of date
    try {
        def struct_field_define = "`a_info` ARRAY<date> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', ['2017-10-01', '2023-09-13', '2023-12-31']), (2, 'doris2', ['1967-10-01', '1000-09-13']); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', ['0001-10-01', null, '0000-01-01']); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, '2017-10-01', '2023-09-13', '2023-12-31']); """

        // test base data
        qt_select_base_date """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_date """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 4. test NULL ARRAY of datetime
    try {
        def struct_field_define = "`a_info` ARRAY<datetime> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', ['2017-10-01 00:00:00', '2011-10-01 01:23:59']), (2, 'doris2', ['2017-10-01 00:00:00', '2011-10-01 01:23:59']); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', ['2017-10-01 00:00:00', null, '2017-10-01 00:00:00']); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, '2017-10-01 00:00:00', '2011-10-01 01:23:59']); """

        // test base data
        qt_select_base_datetime """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_datetime """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


   

    // 5. test NULL ARRAY of VARCHAR(40)
    try {
        def struct_field_define = "`a_info` ARRAY<VARCHAR(40)> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', ['2017-10-01 00:00:00', '2011-10-01 01:23:59']), (2, 'doris2', ['2017-10-01 00:00:00.123', '2011-10-01 01:23:59']); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', ['2017-10-01 00:00:00.123456', null, '2017-10-01 00:00:00.123']); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, 'null', null, '2017-10-01 00:00:00', '2011-10-01 01:23:59']); """

        // test base data
        qt_select_base_varchar """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_varchar """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    

    // 7. test NULL ARRAY of SMALLINT
    try {
        def struct_field_define = "`a_info` ARRAY<SMALLINT> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [-32768, 32767]), (2, 'doris2', [-1, -1, -2, 0 ,3, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-32768, 32767, 99, -99]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, -32768, 32767]); """

        // test base data
        qt_select_base_smallint """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_smallint """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 8. test NULL ARRAY of TINYINT
    try {
        def struct_field_define = "`a_info` ARRAY<TINYINT> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [-128, 127]), (2, 'doris2', [-1, -1, -2, 0 ,3, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-128, 127, 99, -99]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, -128, 127]); """

        // test base data
        qt_select_base_tinyint """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_tinyint """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 9. test NULL ARRAY of boolean
    try {
        def struct_field_define = "`a_info` ARRAY<boolean> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [true, false, true, true, false]), (2, 'doris2', [1, 0, false, true, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [true, false, true]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, false, true]); """

        // test base data
        qt_select_base_boolean """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_boolean """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 10. test NULL ARRAY of bigint
    try {
        def struct_field_define = "`a_info` ARRAY<bigint> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [-9223372036854775808, 9223372036854775807]), (2, 'doris2', [-14141, -9223372036854775808, 9223372036854775807, 9891912 ,3, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-128, 127, 99, -99]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, -9223372036854775808, 9223372036854775807]); """

        // test base data
        qt_select_base_bigint """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_bigint """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 11. test NULL ARRAY of largeint
    try {
        def struct_field_define = "`a_info` ARRAY<largeint> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727]), (2, 'doris2', [-1, 170141183460469231731687303715884105727, -2, 0 ,3, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-170141183460469231731687303715884105728, 127, 99, -99]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, -170141183460469231731687303715884105728, 170141183460469231731687303715884105727]); """

        // test base data
        qt_select_base_largeint """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_largeint """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 12. test NULL ARRAY of float
    try {
        def struct_field_define = "`a_info` ARRAY<float> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [${Float.MIN_VALUE}, ${Float.MAX_VALUE}]), (2, 'doris2', [-1.1, 1.2231, ${Float.MAX_VALUE}, 0 ,3, 99.00989]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-12.8, ${Float.MIN_VALUE}, ${Float.MAX_VALUE}, -9.9]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, ${Float.MIN_VALUE}, ${Float.MAX_VALUE}]); """

        // test base data
        qt_select_base_float """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_float """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 13. test NULL ARRAY of double
    try {
        def struct_field_define = "`a_info` ARRAY<double> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [${Double.MIN_VALUE}, ${Double.MAX_VALUE}]), (2, 'doris2', [-1.1, 1.2231, ${Double.MAX_VALUE}, 0 ,3, 99.00989]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-128, ${Double.MIN_VALUE}, 99, -99]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, ${Double.MIN_VALUE}, ${Double.MAX_VALUE}]); """

        // test base data
        qt_select_base_double """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_double """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 14. test NULL ARRAY of CHAR(10)
    try {
        def struct_field_define = "`a_info` ARRAY<CHAR(10)> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', ['1234567890', 'doris12345']), (2, 'doris2', ['90', 'doris1245']); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', ['doris-123', 'doris-123', 'doris-124', 'doris12378']); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, 'doris-123', 'doris-123']); """

        // test base data
        qt_select_base_CHAR """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_CHAR """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 15. test NULL ARRAY of decimal
    try {
        def struct_field_define = "`a_info` ARRAY<decimal> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [-128.1234567, 127.123456789]), (2, 'doris2', [-1.2, -1.933445, -21231.12, 0.0909 ,3, 99]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [-12.8, 1.27, 9434364.12319, -99.12314]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, -12.8, 12.7]); """

        // test base data
        qt_select_base_decimal """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load_decimal """ SELECT * FROM S3 (
                "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }
}
