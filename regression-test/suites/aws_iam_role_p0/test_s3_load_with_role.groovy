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

import com.google.common.base.Strings;

suite("test_s3_load_with_role") {
    if (Strings.isNullOrEmpty(context.config.regressionAwsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def loadLabel = "label_" + randomStr
    def tableName = "test_s3_load_with_role"

    sql """
        DROP TABLE IF EXISTS ${tableName} FORCE;
        """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        """

    def endpoint = context.config.regressionAwsEndpoint
    def region = context.config.regressionAwsRegion
    def bucket = context.config.regressionAwsBucket
    def roleArn = context.config.regressionAwsRoleArn
    def externalId = context.config.regressionAwsExternalId

    sql """
        LOAD LABEL ${loadLabel} (
            DATA INFILE("s3://${bucket}/regression/tpch/sf0.01/customer.csv.gz")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
        )
        WITH S3 (
            "AWS_ENDPOINT" = "${endpoint}",
            "AWS_REGION" = "${region}",
            "AWS_ROLE_ARN" = "${roleArn}",
            "AWS_EXTERNAL_ID" = "${externalId}",
            "compress_type" = "GZ"
        )
        properties(
            "timeout" = "28800",
            "exec_mem_limit" = "8589934592"
        )
    """

    def maxTryMs = 600000
    while (maxTryMs > 0) {
        String[][] result = sql """ show load where label="${loadLabel}" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load ${loadLabel} finished: $result")
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "Load ${loadLabel} cancelled: ${result}")
            break
        }
        Thread.sleep(5000)
        maxTryMs -= 5000
        if (maxTryMs <= 0) {
            assertTrue(false, "Load ${loadLabel} timeout")
        }
    }

    def result = sql """ select count(*) from ${tableName}; """
    logger.info("result:${result}");
    assertTrue(result[0][0] == 1500)


    def randomStr2 = UUID.randomUUID().toString().replace("-", "")
    def loadLabel2 = "label_" + randomStr2

    sql """
        LOAD LABEL ${loadLabel2} (
            DATA INFILE("s3://${bucket}/regression/tpch/sf0.01/customer.csv.gz")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
        )
        WITH S3 (
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "${region}",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id" = "${externalId}",
            "compress_type" = "GZ"
        )
        properties(
            "timeout" = "28800",
            "exec_mem_limit" = "8589934592"
        )
    """

    maxTryMs = 600000
    while (maxTryMs > 0) {
        String[][] result2 = sql """ show load where label="${loadLabel2}" order by createtime desc limit 1; """
        if (result2[0][2].equals("FINISHED")) {
            logger.info("Load ${loadLabel2} finished: $result2")
            break
        }
        if (result2[0][2].equals("CANCELLED")) {
            assertTrue(false, "Load ${loadLabel2} cancelled: ${result2}")
            break
        }
        Thread.sleep(5000)
        maxTryMs -= 5000
        if (maxTryMs <= 0) {
            assertTrue(false, "Load ${loadLabel2} timeout")
        }
    }

    result = sql """ select count(*) from ${tableName}; """
    logger.info("result:${result}");
    assertTrue(result[0][0] == 3000)

}