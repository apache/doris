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

suite("test_alter_vault_concurrently", "nonConcurrent") {
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
    def s3VaultName = "s3_" + randomStr

    sql """
        CREATE STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "${s3VaultName}",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}",
            "use_path_style" = "false"
        );
    """

    def future1 = thread("threadName1") {
        try_sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "VAULT_NAME" = "${s3VaultName}_1"
            );
            """
    }

    def future2 = thread("threadName2") {
        try_sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "VAULT_NAME" = "${s3VaultName}_2"
            );
            """
    }

    def combineFuture = combineFutures(future1, future2)
    List<List<List<Object>>> result = combineFuture.get()
    logger.info("${result}")

    def hitNum = 0
    def vaultsInfo = try_sql """ SHOW STORAGE VAULTS """
    def newS3VaultName = null

    for (int i = 0; i < vaultsInfo.size(); i++) {
        def name = vaultsInfo[i][0]
        if (name.contains(s3VaultName)) {
            hitNum++
            newS3VaultName = name
            assertTrue(name.equalsIgnoreCase("${s3VaultName}_1") || name.equalsIgnoreCase("${s3VaultName}_2"))
        }
    }
    assertEquals(hitNum, 1)

    future1 = thread("threadName1") {
        try_sql """
            ALTER STORAGE VAULT ${newS3VaultName}
            PROPERTIES (
                "type"="S3",
                "VAULT_NAME" = "${s3VaultName}_1"
                "s3.access_key" = "error_ak_1",
                "s3.secret_key" = "error_sk_1"
            );
            """
    }

    future2 = thread("threadName2") {
        try_sql """
            ALTER STORAGE VAULT ${newS3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.access_key" = "error_ak_2",
                "s3.secret_key" = "error_sk_2"
            );
            """
    }

    combineFuture = combineFutures(future1, future2)
    result = combineFuture.get()
    logger.info("${result}")

    vaultsInfo = try_sql """ SHOW STORAGE VAULTS """
    def found = false
    for (int i = 0; i < vaultsInfo.size(); i++) {
        def name = vaultsInfo[i][0]
        if (name.contains(newS3VaultName)) {
            logger.info("${vaultsInfo[i]}");
            assertTrue(vaultsInfo[i][2].contains("error_ak_1") || vaultsInfo[i][2].contains("error_ak_2"))
            found = true
        }
    }
    assertTrue(found)
}
