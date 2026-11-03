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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_load_iam") {
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def cases = config.authCases.collect { authCase ->
        return [
                name: authCase.name,
                properties: """
                    ${authCase.storageSqlProperties}
                """
        ]
    }
    if (config.provider == "AWS") {
        cases.add(0, [
                name: "aws_role_legacy_properties",
                properties: """
                    "AWS_ENDPOINT" = "${config.endpoint}",
                    "AWS_REGION" = "${config.region}",
                    "AWS_ROLE_ARN" = "${config.roleArn}",
                    "AWS_EXTERNAL_ID" = "${config.externalId ?: ''}"
                """
        ])
        cases[1].name = "aws_role_s3_properties"
    }

    cases.eachWithIndex { testCase, index ->
        def randomStr = UUID.randomUUID().toString().replace("-", "")
        def loadLabel = "iam_load_${index}_${randomStr}"
        def tableName = "test_iam_load_${index}_${randomStr}"

        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """
            CREATE TABLE ${tableName} (
                C_CUSTKEY INTEGER NOT NULL,
                C_NAME VARCHAR(25) NOT NULL,
                C_ADDRESS VARCHAR(40) NOT NULL,
                C_NATIONKEY INTEGER NOT NULL,
                C_PHONE CHAR(15) NOT NULL,
                C_ACCTBAL DECIMAL(15, 2) NOT NULL,
                C_MKTSEGMENT CHAR(10) NOT NULL,
                C_COMMENT VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """

        logger.info("run ${name} with ${testCase.name}")
        sql """
            LOAD LABEL ${loadLabel} (
                DATA INFILE("${config.scheme}://${config.bucket}/${config.dataPath}")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
            )
            WITH S3 (
                ${testCase.properties},
                "compress_type" = "GZ"
            )
            PROPERTIES(
                "timeout" = "600",
                "exec_mem_limit" = "8589934592"
            )
        """

        def maxTryMs = 600000
        while (maxTryMs > 0) {
            String[][] loadResult = sql """SHOW LOAD WHERE LABEL = "${loadLabel}"
                    ORDER BY CREATETIME DESC LIMIT 1"""
            if (loadResult[0][2] == "FINISHED") {
                break
            }
            if (loadResult[0][2] == "CANCELLED") {
                assertTrue(false, "Load ${loadLabel} cancelled for ${testCase.name}: ${loadResult}")
            }
            Thread.sleep(5000)
            maxTryMs -= 5000
        }
        assertTrue(maxTryMs > 0, "Load ${loadLabel} timeout for ${testCase.name}")

        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(1500, result[0][0], testCase.name)
    }
}
