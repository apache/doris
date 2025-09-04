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

suite("test_outfile_s3_storage", "p0,external,external_docker") {
    String enabled = context.config.otherConfigs.get("enableRefactorParamsTest")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        return
    }
    def export_table_name = "test_outfile_s3_storage"

    def s3_tvf = {bucket, s3_endpoint, region, ak, sk, path ->
        // http schema
        order_qt_s3_tvf_1_http """ SELECT * FROM S3 (
            "uri" = "http://${bucket}.${s3_endpoint}${path}0.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "parquet",
            "region" = "${region}"
        );
        """
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

    def getConfigOrDefault = { String key, String defaultValue ->
        def value = context.config.otherConfigs.get(key)
        if (value == null || value.isEmpty()) {
            return defaultValue
        }
        return value
    }

    String ak = ""
    String sk = ""
    String s3_endpoint = ""
    String region = ""
    String bucket = ""

    /*******************************************************************************************************
     *****************************      TEST AWS      *****************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("AWSAK")
        sk = context.config.otherConfigs.get("AWSSK")
        s3_endpoint = getConfigOrDefault("AWSEndpoint","s3.ap-northeast-1.amazonaws.com")
        region = getConfigOrDefault ("AWSRegion","ap-northeast-1")
        bucket = getConfigOrDefault ("AWSS3Bucket","selectdb-qa-datalake-test")

        // 1. test s3 schema
        def outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
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
        def outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));

        // 2. test AWS_ENDPOINT
        outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
        res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS parquet
            PROPERTIES (
                "AWS_ENDPOINT" = "${s3_endpoint}",
                "AWS_REGION" = "${region}",
                "AWS_SECRET_KEY"="${sk}",
                "AWS_ACCESS_KEY" = "${ak}"
            );
        """
        outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));

    } finally {
    }

    /*******************************************************************************************************
     *****************************      TEST COS & COSN    *************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("txYunAk")
        sk = context.config.otherConfigs.get("txYunSk")
        s3_endpoint = getConfigOrDefault("txYunEndpoint","cos.ap-beijing.myqcloud.com")
        region = getConfigOrDefault ("txYunRegion","ap-beijing");
        bucket = getConfigOrDefault ("txYunBucket","doris-build-1308700295")

        // 1. test s3 schema
        def outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
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
        def outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));

        // 2. test AWS_ENDPOINT
        outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
        res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS parquet
            PROPERTIES (
                "AWS_ENDPOINT" = "${s3_endpoint}",
                "AWS_REGION" = "${region}",
                "AWS_SECRET_KEY"="${sk}",
                "AWS_ACCESS_KEY" = "${ak}"
            );
        """
        outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));

    } finally {
    }

    /*******************************************************************************************************
     *****************************      TEST OSS    ********************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("aliYunAk")
        sk = context.config.otherConfigs.get("aliYunSk")
        s3_endpoint = getConfigOrDefault("aliYunEndpoint","oss-cn-hongkong.aliyuncs.com")
        region = getConfigOrDefault ("aliYunRegion","oss-cn-hongkong")
        bucket = getConfigOrDefault ("aliYunBucket","doris-regression-hk");

        // 1. test s3 schema
        def outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
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
        def outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));

        // 2. test AWS_ENDPOINT
        outFilePath = "${bucket}/test_outfile_s3_storage/exp_"
        res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS parquet
            PROPERTIES (
                "AWS_ENDPOINT" = "${s3_endpoint}",
                "AWS_REGION" = "${region}",
                "AWS_SECRET_KEY"="${sk}",
                "AWS_ACCESS_KEY" = "${ak}"
            );
        """
        outfile_url = res[0][3];
        s3_tvf(bucket, s3_endpoint, region, ak, sk, outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1));
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
    } finally {
    }

}
