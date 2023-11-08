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

suite("test_s3_tvf", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "test_s3_tvf_export_test"
    def outFilePath = "${bucket}/test_s3_tvf/export_test/exp_"


    def create_table = {table_name ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `name` STRING COMMENT "用户名称",
            `age` INT COMMENT "用户年龄",
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

    // create table to export data
    create_table(export_table_name)

    // insert data
    sql """ insert into ${export_table_name} values (1, 'doris1', 18); """
    sql """ insert into ${export_table_name} values (2, 'doris2', 19); """
    sql """ insert into ${export_table_name} values (3, 'doris3', 99); """
    sql """ insert into ${export_table_name} values (4, 'doris4', null); """
    sql """ insert into ${export_table_name} values (5, 'doris5', 15); """

    // test base data
    qt_select_base """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

    // test outfile to s3
    def outfile_url = outfile_to_S3()

    // 1. normal
    try {
        order_qt_select_1 """ SELECT * FROM S3 (
                            "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.orc",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "orc",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }


    // 2. test endpoint property
    try {
        order_qt_select_2 """ SELECT * FROM S3 (
                            "uri" = "http://${outfile_url.substring(5)}0.orc",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "s3.endpoint" = "${s3_endpoint}",
                            "format" = "orc",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }

    // 3.test use_path_style
    try {
        order_qt_select_3 """ SELECT * FROM S3 (
                            "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.orc",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "orc",
                            "use_path_style" = "true",
                            "region" = "${region}"
                        );
                        """
    } finally {
    }
}
