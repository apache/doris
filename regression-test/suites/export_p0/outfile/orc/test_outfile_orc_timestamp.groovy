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

suite("test_outfile_orc_timestamp", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "outfile_orc_complex_type_export_test"
    def outFilePath = "${bucket}/outfile/orc/complex_type/exp_"


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
            FORMAT AS ORC
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }

    // 1. test datetimev2 NULL type
    try {

        def field_define = """
                                    `datetime_1` datetimev2(1) NULL,
                                    `datetime_2` datetimev2(2) NULL,
                                    `datetime_3` datetimev2(3) NULL,
                                    `datetime_4` datetimev2(4) NULL,
                                    `datetime_5` datetimev2(5) NULL,
                                    `datetime_6` datetimev2(6) NULL
                                """
        // create table to export data
        create_table(export_table_name, field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', '0000-01-01 00:00:00.1', '0000-01-01 00:00:00.12', '0000-01-01 00:00:00.123', '0000-01-01 00:00:00.1234', '0000-01-01 00:00:00.12345', '0000-01-01 00:00:00.123456'); """
        sql """ insert into ${export_table_name} values (2, 'doris2', '2000-07-03 00:00:00.1', '2000-07-03 00:00:00.99', '2000-07-03 00:00:00.111', '2000-07-03 00:00:00.1234', '2000-07-03 00:00:00.12345', '2000-07-03 02:01:00.999999'); """
        sql """ insert into ${export_table_name} values (3, 'doris3', '1070-07-03 00:00:00.1', '1970-07-03 00:00:00.45', '1970-07-03 00:00:00.999', '2000-07-03 00:00:00.1234', '1970-07-03 00:00:00.12345', '1234-07-03 11:12:13.111111'); """
        sql """ insert into ${export_table_name} values (4, 'doris4', '0021-01-01 12:23:59.1', '1001-05-01 11:01:11.00', '2000-11-03 12:12:21.000', '1900-12-31 23:54:21.1234', '1970-01-01 08:00:00.12345', '1969-07-03 09:09:09.777123'); """
        sql """ insert into ${export_table_name} values (5, 'doris5', '2000-07-03 00:00:00.1', '2000-07-03 00:00:00.01', '2000-07-03 00:00:00.555', '1970-01-01 02:10:05.1234', '0001-03-01 00:10:05.12345', '0000-01-01 08:00:00.123456'); """
        sql """ insert into ${export_table_name} values (6, 'doris6', '2000-07-03 00:00:00.1', '1900-12-31 23:54:17.77', '1900-12-31 23:54:16.321', '2000-07-03 00:00:00.1234', '1900-12-31 23:54:19.12345', '2000-07-03 00:00:00.123456'); """
        sql """ insert into ${export_table_name} values (7, null, '2000-07-03 00:00:00.1', '2000-07-03 00:00:00.12', '1900-12-31 23:54:20.123', '2000-07-03 00:00:00.1234', '2000-07-03 00:00:00.12345', '2000-07-03 00:00:00.123456'); """
        sql """ insert into ${export_table_name} values (8, null, '2000-07-03 00:00:00.1', '1969-07-03 00:00:00.12', '2000-07-03 00:00:00.123', '2000-07-03 00:00:00.1234', '2000-07-03 00:00:00.12345', '2000-07-03 00:00:00.123456'); """

        // test base data
        qt_select_base """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        // test outfile to s3
        def outfile_url = outfile_to_S3()

        qt_select_tvf1 """ SELECT * FROM S3 (
                            "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.orc",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "orc",
                            "region" = "${region}"
                        );
                        """

    } finally {
    }
}