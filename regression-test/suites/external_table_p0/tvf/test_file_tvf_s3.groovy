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

suite("test_file_tvf_s3", "p0") {

    def export_table_name = "test_file_tvf_s3"

    def outfile_to_S3 = {bucket, s3_endpoint, region, ak, sk  ->
        def outFilePath = "${bucket}/outfile_different_s3/exp_"
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS parquet
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        return res[0][3]
    }


    sql """ DROP TABLE IF EXISTS ${export_table_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${export_table_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `Name` STRING COMMENT "用户年龄",
        `Age` int(11) NULL
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 10; i ++) {
        sb.append("""
            (${i}, 'ftw-${i}', ${i + 18}),
        """)
    }
    sb.append("""
            (${i}, NULL, NULL)
        """)
    sql """ INSERT INTO ${export_table_name} VALUES
            ${sb.toString()}
        """
    qt_select_export """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """


    String ak = ""
    String sk = ""
    String s3_endpoint = ""
    String region = ""
    String bucket = ""
    String outfile_url = ""

    def file_s3_tvf = {uri_prefix, endpoint_key, ak_key, sk_key, region_key, is_path_style  ->
        // http schema
        order_qt_s3_tvf """ SELECT * FROM FILE (
            "uri" = "${uri_prefix}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "${endpoint_key}" = "${s3_endpoint}",
            "${ak_key}"= "${ak}",
            "${sk_key}" = "${sk}",
            "${region_key}" = "${region}",
            "use_path_style" = "${is_path_style}",
            "format" = "parquet"
        );
        """
    }



    /*******************************************************************************************************
     *****************************      TEST COS & COSN    *************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("txYunAk")
        sk = context.config.otherConfigs.get("txYunSk")
        s3_endpoint = "cos.ap-beijing.myqcloud.com"
        region = "ap-beijing"
        bucket = "doris-build-1308700295";

        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "s3.region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "true");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "s3.region", "true");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");

    } finally {
    }

     /*******************************************************************************************************
     *****************************      TEST OSS    ********************************************************
     *******************************************************************************************************/
     try {
        ak = context.config.otherConfigs.get("aliYunAk")
        sk = context.config.otherConfigs.get("aliYunSk")
        s3_endpoint = "oss-cn-hongkong.aliyuncs.com"
        region = "oss-cn-hongkong"
        bucket = "doris-regression-hk";


        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "s3.region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");

        file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");

        file_s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");

     } finally {

     }


    /*******************************************************************************************************
     *****************************      TEST OBS    ********************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("hwYunAk")
        sk = context.config.otherConfigs.get("hwYunSk")
        s3_endpoint = "obs.cn-north-4.myhuaweicloud.com"
        region = "cn-north-4"
        bucket = "doris-build";


        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "s3.region", "false");
        file_s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "true");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "s3.region", "true");
        file_s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
    } finally {
    }

    /*******************************************************************************************************
     *****************************      TEST AWS      *****************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("AWSAK")
        sk = context.config.otherConfigs.get("AWSSK")
        s3_endpoint = "s3.ap-northeast-1.amazonaws.com"
        region = "ap-northeast-1"
        bucket = "selectdb-qa-datalake-test"

        if (ak != null && sk != null) {
            outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

            file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "region", "false");
            file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
            file_s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "s3.region", "false");
            file_s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
            file_s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "true");
            file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
            file_s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "s3.region", "true");
            file_s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
            file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
            file_s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "region", "false");
        }
    } finally {
    }
}