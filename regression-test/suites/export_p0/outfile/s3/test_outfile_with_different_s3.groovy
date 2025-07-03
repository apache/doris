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

suite("test_outfile_with_different_s3", "p0") {

    def export_table_name = "test_outfile_with_different_s3"
    
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


    // 1. test s3
    try {
        String ak = getS3AK()
        String sk = getS3SK()
        String s3_endpoint = getS3Endpoint()
        String region = getS3Region()
        String bucket = context.config.otherConfigs.get("s3BucketName");

        def outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        // http schema
        qt_s3_tvf_1_http """ SELECT * FROM S3 (
            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "parquet",
            "region" = "${region}"
        );
        """
        // s3 schema
        qt_s3_tvf_1_s3 """ SELECT * FROM S3 (
            "uri" = "s3://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "region" = "${region}"
        );
        """
    } finally {
    }

    // 2. test cos & cosn
    try {
        String ak = context.config.otherConfigs.get("txYunAk")
        String sk = context.config.otherConfigs.get("txYunSk")
        String s3_endpoint = "cos.ap-beijing.myqcloud.com"
        String region = "ap-beijing"
        String bucket = "doris-build-1308700295";

        def outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        // http schema
        qt_s3_tvf_2_http """ SELECT * FROM S3 (
            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "parquet",
            "s3.region" = "${region}"
        );
        """

        // s3 schema
        qt_s3_tvf_2_s3 """ SELECT * FROM S3 (
            "uri" = "s3://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "region" = "${region}"
        );
        """

        // cos schema
        qt_s3_tvf_2_cos """ SELECT * FROM S3 (
            "uri" = "cos://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "s3.region" = "${region}"
        );
        """

        // cosn schema
        qt_s3_tvf_2_cosn """ SELECT * FROM S3 (
            "uri" = "cosn://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "s3.region" = "${region}"
        );
        """
    } finally {
    }

    // 3. test obs
    try {
        String ak = context.config.otherConfigs.get("hwYunAk")
        String sk = context.config.otherConfigs.get("hwYunSk")
        String s3_endpoint = "obs.cn-north-4.myhuaweicloud.com"
        String region = "cn-north-4"
        String bucket = "doris-build";
        
        def outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        // http schema
        qt_s3_tvf_3_http """ SELECT * FROM S3 (
            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "parquet",
            "s3.region" = "${region}"
        );
        """

        // s3 schema
        qt_s3_tvf_3_s3 """ SELECT * FROM S3 (
            "uri" = "s3://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "region" = "${region}"
        );
        """

        // obs schema
        qt_s3_tvf_3_obs """ SELECT * FROM S3 (
            "uri" = "obs://${bucket}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3_endpoint}",
            "format" = "parquet",
            "s3.region" = "${region}"
        );
        """
    } finally {
    }
}
