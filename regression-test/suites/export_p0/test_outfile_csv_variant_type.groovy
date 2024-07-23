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

suite("test_outfile_csv_variant_type", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "outfile_csv_variant_export_test"
    def load_table_name = "outfile_csv_variant_type_load_test"
    def outFilePath = "${bucket}/outfile/csv/variant_type/exp_"


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
            FORMAT AS CSV
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }


    // 1. test NULL variant 
    try {
        def struct_field_define = "`a_info` VARIANT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', '[9, 99, 999]'), (2, 'doris2', '[8, 88]'); """
        sql """ insert into ${export_table_name} values (3, 'doris3', '{"a" : 123}'); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', '[1, null, 2]'); """
        sql """ insert into ${export_table_name} values (6, 'doris6', '{"aaaa" : "111111"}'); """
        sql """ insert into ${export_table_name} values (7, 'doris7', '{"bbbb" : 1.1111}'); """
        sql """ insert into ${export_table_name} values (8, 'doris8', '{"xxx" : [111.11]}'); """


        // test base data
        qt_select_base1 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load1 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 2. test NOT NULL VARIANT
    try {
        def struct_field_define = "`a_info` VARIANT NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', '[9, 99, 999]'), (2, 'doris2', '[8, 88]'); """
        sql """ insert into ${export_table_name} values (3, 'doris3', '{"a" : 123}'); """
        sql """ insert into ${export_table_name} values (4, 'doris4', '{}'); """
        sql """ insert into ${export_table_name} values (5, 'doris5', '[1, null, 2]'); """
        sql """ insert into ${export_table_name} values (6, 'doris6', '{"aaaa" : "111111"}'); """
        sql """ insert into ${export_table_name} values (7, 'doris7', '{"bbbb" : 1.1111}'); """
        sql """ insert into ${export_table_name} values (8, 'doris8', '{"xxx" : [111.11]}'); """

        // test base data
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load2 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "csv",
                "region" = "${region}"
            );
            """
    } finally {
    }
}
