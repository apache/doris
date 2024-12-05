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

suite("test_alter_s3_vault", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${suiteName}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "${suiteName}",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "s3.bucket" = "error_bucket"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "provider" = "${getS3Provider()}"
            );
        """
    }, "Alter property")


    def vaultName = suiteName
    String properties;

    def vaultInfos = try_sql """show storage vault"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        if (name.equals(vaultName)) {
            properties = vaultInfos[i][2]
        }
    }

    def newVaultName = suiteName + "_new";

    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="S3",
            "VAULT_NAME" = "${newVaultName}",
            "s3.access_key" = "new_ak"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULT;"""
    boolean exist = false

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(vaultName)) {
            assertTrue(false);
        }
        if (name.equals(newVaultName)) {
            assertTrue(vaultInfos[i][2].contains("new_ak"))
            exist = true
        }
    }
    assertTrue(exist)
    // failed to insert due to the wrong ak
    expectExceptionLike({ sql """insert into alter_s3_vault_tbl values("2", "2");""" }, "")
}
