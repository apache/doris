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

import java.util.stream.Collectors;
import java.util.stream.Stream;

suite("test_default_vault_concurrently", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case, because storage vault not enabled")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def s3VaultName1 = "s3_" + randomStr + "_1"
    def s3VaultName2 = "s3_" + randomStr + "_2"

    sql """
        CREATE STORAGE VAULT ${s3VaultName1}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "${s3VaultName1}",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}",
            "use_path_style" = "false"
        );
    """

    sql """
        CREATE STORAGE VAULT ${s3VaultName2}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "${s3VaultName2}",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}",
            "use_path_style" = "false"
        );
    """

    def future1 = thread("threadName1") {
        for (int i = 0; i < 200; i++) {
            sql """SET ${s3VaultName1} AS DEFAULT STORAGE VAULT;"""
        }
    }

    def future2 = thread("threadName2") {
        for (int i = 0; i < 200; i++) {
            sql """SET ${s3VaultName2} AS DEFAULT STORAGE VAULT;"""
        }
    }

    def combineFuture = combineFutures(future1, future2)

    List<List<List<Object>>> result = combineFuture.get()
    logger.info("${result}")

    def vaultsInfo = try_sql """ SHOW STORAGE VAULTS """
    def found = false
    def defaultVaultName = null
    for (int i = 0; i < vaultsInfo.size(); i++) {
        def name = vaultsInfo[i][0]
        def isDefault = vaultsInfo[i][3]
        if (isDefault.equalsIgnoreCase("true")) {
            assertFalse(found)
            found = true
            defaultVaultName = name;
            assertTrue(name.equalsIgnoreCase(s3VaultName1) || name.equalsIgnoreCase(s3VaultName2))
        }
    }
    assertTrue(found)

    sql """
        CREATE TABLE ${defaultVaultName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    future1 = thread("threadName1") {
        for (int i = 0; i < 50; i++) {
            sql """ insert into ${defaultVaultName} values(${i}, ${i}); """
        }
    }

    future2 = thread("threadName2") {
        sql """ UNSET DEFAULT STORAGE VAULT; """
    }

    combineFuture = combineFutures(future1, future2)

    result = combineFuture.get()
    logger.info("${result}")
}
