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

suite("test_read_csv_empty_line_as_null", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "test_read_csv_empty_line"
    def outFilePath = "${bucket}/test_read_csv_empty_line/exp_"


    def create_table = {table_name ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
                `id` INT NULL,
                `content` varchar(32) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_S3 = {
        // select ... into outfile ...
        def res = sql """
            SELECT content FROM ${export_table_name} t ORDER BY id
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

    // create table to export data
    create_table(export_table_name)

    // insert data
    sql """ insert into ${export_table_name} values (1, "1,doris1,16"); """
    sql """ insert into ${export_table_name} values (2, "2,doris2,18"); """
    sql """ insert into ${export_table_name} values (3, ""); """
    sql """ insert into ${export_table_name} values (4, "3,doris3,19"); """
    sql """ insert into ${export_table_name} values (5, ""); """
    sql """ insert into ${export_table_name} values (6, ""); """
    sql """ insert into ${export_table_name} values (7, ""); """
    sql """ insert into ${export_table_name} values (8, ""); """
    sql """ insert into ${export_table_name} values (9, "4,doris4,20"); """
    sql """ insert into ${export_table_name} values (10, ""); """

    // test base data
    qt_select_base """ SELECT content FROM ${export_table_name} t ORDER BY id; """

    // test outfile to s3
    def outfile_url = outfile_to_S3()

    // test read_csv_empty_line_as_null = false
    try {
        order_qt_select_1 """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }

    // test read_csv_empty_line_as_null = true
    try {
        sql """ set read_csv_empty_line_as_null=true; """
        order_qt_select_1 """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "column_separator" = ",",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }
}