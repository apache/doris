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
import static groovy.test.GroovyAssert.shouldFail

suite("test_s3_tvf_s3_storage", "p0,external,external_docker") {
    String enabled = context.config.otherConfigs.get("enableRefactorParamsTest")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        return
    }
    def export_table_name = "test_s3_tvf_s3_storage"

    def outfile_to_S3 = { bucket, s3_endpoint, region, ak, sk ->
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
    for (; i < 10; i++) {
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
    def insert_result = sql """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """
    assert insert_result.size() == 10

    String ak = ""
    String sk = ""
    String s3_endpoint = ""
    String region = ""
    String bucket = ""
    String outfile_url = ""

    def s3_tvf = { uri_prefix, endpoint_key, ak_key, sk_key, region_key, is_path_style ->
        // http schema
        def queryResult= sql """ SELECT * FROM S3 (
            "uri" = "${uri_prefix}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
            "${endpoint_key}" = "${s3_endpoint}",
            "${ak_key}"= "${ak}",
            "${sk_key}" = "${sk}",
            "${region_key}" = "${region}",
            "use_path_style" = "${is_path_style}",
            "format" = "parquet"
        );
        """
        assert queryResult.size() == 10
    }

    def getConfigOrDefault = { String key, String defaultValue ->
        def value = context.config.otherConfigs.get(key)
        if (value == null || value.isEmpty()) {
            return defaultValue
        }
        return value
    }
    /*******************************************************************************************************
     *****************************      TEST AWS      *****************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("AWSAK")
        sk = context.config.otherConfigs.get("AWSSK")
        s3_endpoint = getConfigOrDefault("AWSEndpoint","s3.ap-northeast-1.amazonaws.com")
        region = getConfigOrDefault ("AWSRegion","ap-northeast-1")
        bucket = getConfigOrDefault ("AWSS3Bucket","selectdb-qa-datalake-test")

        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "s3.region", "false");

        //s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key" , "s3.secret_key", "region", "true");
        s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key", "s3.secret_key", "region", "true");
        //s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        // s3_tvf("s3://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "s3.region", "true");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "s3.region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

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


        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "s3.region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "cos.access_key", "cos.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "cos.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key", "s3.secret_key", "region", "true");
        shouldFail {
            s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key", "s3.secret_key", "region", "false");
        }

        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "s3.region", "true");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "s3.region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "cos.endpoint", "cos.access_key", "cos.secret_key", "cos.region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "cos.access_key", "cos.secret_key", "cos.region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

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


        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "s3.region", "false");
        shouldFail {
            // it's OSS 
            s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "cos.secret_key", "region", "false");
        }
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        //endpoint field is no valid, so we extract the endpoint from uri
        s3_tvf("http://${bucket}.${s3_endpoint}", "cos.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

        // TODO(ftw): Note this case
        // s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "true");

        // s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        // s3_tvf("s3://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

        // TODO(ftw): Note this case
        // s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "s3.secret_key", "s3.region", "true");

        // s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "region", "false");
        // s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "s3.region", "false");
        // s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY" , "AWS_SECRET_KEY", "AWS_REGION", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        // s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key" , "AWS_SECRET_KEY", "region", "false");
        // s3_tvf("s3://${bucket}", "cos.endpoint", "cos.access_key" , "cos.secret_key", "cos.region", "false");
        // s3_tvf("s3://${bucket}", "s3.endpoint", "cos.access_key" , "cos.secret_key", "cos.region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

    } finally {

    }


    /*******************************************************************************************************
     *****************************      TEST OBS    ********************************************************
     *******************************************************************************************************/
    try {
        ak = context.config.otherConfigs.get("hwYunAk")
        sk = context.config.otherConfigs.get("hwYunSk")
        s3_endpoint = getConfigOrDefault("hwYunEndpoint","obs.cn-north-4.myhuaweicloud.com")
        region = getConfigOrDefault("hwYunRegion","cn-north-4")
        bucket = getConfigOrDefault("hwYunBucket","doris-build");


        outfile_url = outfile_to_S3(bucket, s3_endpoint, region, ak, sk)

        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "s3.region", "false");
        shouldFail {
            s3_tvf("http://${bucket}.${s3_endpoint}", "", "cos.access_key", "s3.secret_key", "region", "false");
        }
        s3_tvf("http://${bucket}.${s3_endpoint}", "", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("http://${bucket}.${s3_endpoint}", "cos.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        shouldFail {
            s3_tvf("http://${bucket}.${s3_endpoint}", "cos.endpoint", "s3.access_key", "s3.secret_key", "region", "true");
        }

        s3_tvf("http://${bucket}.${s3_endpoint}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");

        s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key", "s3.secret_key", "region", "true");
        shouldFail {
            s3_tvf("http://${s3_endpoint}/${bucket}", "", "s3.access_key", "s3.secret_key", "region", "false");
        }
        // should support in 2.1&3.0 s3_tvf("s3://${s3_endpoint}/${bucket}", "", "s3.access_key" , "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "s3.region", "true");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "s3.region", "false");
        s3_tvf("s3://${bucket}", "AWS_ENDPOINT", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_REGION", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "region", "false");
        s3_tvf("s3://${bucket}", "s3.endpoint", "s3.access_key", "AWS_SECRET_KEY", "region", "false");
        shouldFail {
            s3_tvf("s3://${bucket}", "cos.endpoint", "cos.access_key", "cos.secret_key", "cos.region", "false");
        }
        shouldFail{
            s3_tvf("s3://${bucket}", "s3.endpoint", "cos.access_key", "s3.secret_key", "cos.region", "false");
        }
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
        s3_tvf("cos://${bucket}", "s3.endpoint", "s3.access_key", "s3.secret_key", "region", "false");
    } finally {
    }
}
