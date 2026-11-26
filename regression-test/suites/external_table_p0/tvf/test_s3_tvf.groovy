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

suite("test_s3_tvf", "p0,external") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "test_s3_tvf_export_test"
    def outFilePath = "${bucket}/est_s3_tvf/export_test/exp_"


    def create_table = {table_name ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `name` STRING COMMENT "用户名称",
            `age` INT COMMENT "用户年龄"
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
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
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
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "orc",
                            "use_path_style" = "false", -- aliyun does not support path_style
                            "region" = "${region}"
                        );
                        """
    } finally {
    }

    try {
        order_qt_select_4 """ SELECT * FROM S3 (
                            "uri" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_hive_text.text",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "hive_text",
                            "region" = "${region}"
                        ) order by c1,c2,c3;
                        """
    } finally {
    }

    try {
        order_qt_select_5 """ SELECT * FROM S3 (
                            "uri" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_hive_text.text",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "hive_text",
                            "region" = "${region}",
                            "csv_schema"="k1:int;k2:string;k3:double"
                        ) order by k1,k2,k3;
                        """
    } finally {
    }

    try {
        order_qt_select_6 """ SELECT * FROM S3 (
                            "uri" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_hive_text.text",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "hive_text",
                            "region" = "${region}",
                            "csv_schema"="k1:int;k2:string;k3:double"
                        )  where k3 > 1.5  order by k3,k2,k1;
                        """
    } finally {
    }

    try {
        order_qt_select_7 """ SELECT * FROM S3 (
                            "uri" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_hive_text.text",
                            "s3.access_key"= "${ak}",
                            "s3.secret_key" = "${sk}",
                            "format" = "hive_text",
                            "region" = "${region}",
                            "csv_schema"="k1:int;k2:string;k3:double"
                        )  where k1 > 100  order by k3,k2,k1;
                        """
    } finally {
    }

    // Exercise schema conversion with both readers, including failed casts, source NULLs and
    // multiple batches. The BIGINT seed must be listed before the STRING file for TVF inference.
    String castPrefix = "s3://${getS3BucketName()}/regression/parquet_invalid_string_cast/${UUID.randomUUID()}"
    String castStorageProperties = """
        "s3.endpoint" = "${s3_endpoint}",
        "s3.region" = "${region}",
        "s3.access_key" = "${ak}",
        "s3.secret_key" = "${sk}"
    """
    sql """SELECT CAST(1 AS BIGINT) AS id, CAST('seed' AS STRING) AS name
           INTO OUTFILE "${castPrefix}/00_seed/part_" FORMAT AS parquet
           PROPERTIES (${castStorageProperties})"""
    sql """SELECT CAST(CASE number % 4
                          WHEN 0 THEN 'xx'
                          WHEN 1 THEN '5'
                          WHEN 2 THEN NULL
                          ELSE '9' END AS STRING) AS id,
                  CAST(number AS STRING) AS name
           FROM numbers("number" = "4096")
           INTO OUTFILE "${castPrefix}/01_strings/part_" FORMAT AS parquet
           PROPERTIES (${castStorageProperties})"""

    def previousCastSettings = sql "SELECT @@enable_file_scanner_v2, @@batch_size, @@enable_strict_cast"
    try {
        sql "SET batch_size=1024"
        sql "SET enable_strict_cast=false"
        // Materialize every value; COUNT(*) could use footer statistics and skip conversion.
        // Compare the actual reader outputs rather than hardcoding expected rows in Groovy.
        def castResults = [false, true].collect { scannerV2 ->
            sql "SET enable_file_scanner_v2=${scannerV2}"
            sql """SELECT id, name FROM S3(
                       "uri" = "${castPrefix}/{00_seed,01_strings}/*.parquet",
                       "format" = "parquet",
                       ${castStorageProperties}) ORDER BY name"""
        }
        assertEquals(castResults[0], castResults[1],
                "V1 and V2 must preserve the same values and NULLs across mixed-schema batches")
    } finally {
        sql "SET enable_file_scanner_v2=${previousCastSettings[0][0]}"
        sql "SET batch_size=${previousCastSettings[0][1]}"
        sql "SET enable_strict_cast=${previousCastSettings[0][2]}"
    }
}
