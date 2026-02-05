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

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;

suite("test_s3_load_with_tmp_token", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def s3Ak = getS3AK()
    def s3Sk = getS3SK()
    def stsRegion = context.config.otherConfigs.get("regressionAliyunStsRegion")
    def stsRoleArn = context.config.otherConfigs.get("regressionAliyunStsRoleArn")

    String roleSessionName = "groovy-session"
    Long durationSeconds = 3600L

    // try {
    DefaultProfile profile = DefaultProfile.getProfile(stsRegion, s3Ak, s3Sk)
    DefaultAcsClient client = new DefaultAcsClient(profile)
    AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().with {
        it.roleArn = stsRoleArn
        it.roleSessionName = roleSessionName
        it.durationSeconds = durationSeconds
        return it
    }

    AssumeRoleResponse response = client.getAcsResponse(assumeRoleRequest)
    def stsAk = "${response.credentials.accessKeyId}"
    def stsSk = "${response.credentials.accessKeySecret}"
    def stsToken = "${response.credentials.securityToken}"


    def tableName = "test_tmp_token_load"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName}
        (
            k00 INT             NOT NULL,
            k01 DATE            NOT NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)         NULL,
            k11 DECIMALV3(9,1)       NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL,
            kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
            kd02 TINYINT         NOT NULL DEFAULT "1",
            kd03 SMALLINT        NOT NULL DEFAULT "2",
            kd04 INT             NOT NULL DEFAULT "3",
            kd05 BIGINT          NOT NULL DEFAULT "4",
            kd06 LARGEINT        NOT NULL DEFAULT "5",
            kd07 FLOAT           NOT NULL DEFAULT "6.0",
            kd08 DOUBLE          NOT NULL DEFAULT "7.0",
            kd09 DECIMAL         NOT NULL DEFAULT "888888888",
            kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
            kd11 DATE            NOT NULL DEFAULT "2023-08-24",
            kd12 DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
            kd14 DATETIMEV2      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
            kd18 JSON            NULL
        )
        DUPLICATE KEY(k00,k01)
        PARTITION BY RANGE(k01)
        (
            PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
            PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
            PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
        )
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    def path = "s3://${s3BucketName}/regression/load/data/basic_data.csv"
    def format_str = "CSV"
    def data_desc_prefix = """DATA INFILE("$path")
               INTO TABLE $tableName """

    def data_desc_suffix = """COLUMNS TERMINATED BY "|"
               FORMAT AS $format_str
               (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18) """

    sql """ truncate table ${tableName} """
    def label = UUID.randomUUID().toString().replace("-", "0")
    def sql_str = """
        LOAD LABEL $label (
            $data_desc_prefix
            $data_desc_suffix
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$stsAk",
            "AWS_SECRET_KEY" = "$stsSk",
            "AWS_TOKEN" = "$stsToken",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
            "max_filter_ratio" = "1.0"
        )
        """

    sql """${sql_str}"""

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED: " + result)
            break
        } else if (result[0][2].equals("CANCELLED")) {
            logger.error("Load CANCELLED: " + result)
            assertTrue(false, "Load job was cancelled. Reason: ${result[0][7]}")
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(false, "Load Timeout: $label")
        }
    }

    qt_sql """ SELECT COUNT(*) FROM ${tableName} """

    sql """ truncate table ${tableName} """

    def tvf_sql = """
        INSERT INTO ${tableName} 
        (k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)
        SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19
        FROM S3(
            "uri" = "${path}",
            "s3.access_key" = "${stsAk}",
            "s3.secret_key" = "${stsSk}",
            "s3.session_token" = "${stsToken}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}",
            "provider" = "${getS3Provider()}",
            "format" = "csv",
            "column_separator" = "|",
            "csv_schema" = "c1:int;c2:date;c3:boolean;c4:tinyint;c5:smallint;c6:int;c7:bigint;c8:largeint;c9:float;c10:double;c11:decimal(9,1);c12:decimal(9,1);c13:datetime;c14:date;c15:datetime;c16:string;c17:string;c18:string;c19:string"
        )
    """
    
    logger.info("Submit TVF SQL: ${tvf_sql}")
    sql """${tvf_sql}"""
    
    qt_sql_tvf """ SELECT COUNT(*) FROM ${tableName} """
}