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

    def dupVaultName = "${suiteName}" + "_dup"
    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${dupVaultName}
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

    sql """
        DROP TABLE IF EXISTS alter_s3_vault_tbl
        """

    sql """
        CREATE TABLE IF NOT EXISTS alter_s3_vault_tbl
        (
        `k1` INT NULL,
        `v1` INT NULL
        )
        UNIQUE KEY (k1)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true",
        "storage_vault_name" = "${suiteName}"
        );
    """

    sql """insert into alter_s3_vault_tbl values(2, 2); """

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

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "s3.access_key" = "new_ak"
            );
        """
    }, "Accesskey and secretkey must be alter together")

    def vaultName = suiteName
    def String properties;

    def vaultInfos = try_sql """show storage vaults"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(vaultName)) {
            properties = vaultInfos[i][2]
        }
    }

    // alter ak sk
    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="S3",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULT;"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(vaultName)) {
            def newProperties = vaultInfos[i][2]
            assert properties == newProperties, "Properties are not the same"
        }
    }

    sql """insert into alter_s3_vault_tbl values("2", "2"); """


    // rename
    newVaultName = vaultName + "_new";

    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="S3",
            "VAULT_NAME" = "${newVaultName}"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULT;"""
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(newVaultName)) {
            def newProperties = vaultInfos[i][2]
            assert properties == newProperties, "Properties are not the same"
        }
        if (name.equals(vaultName)) {
            assertTrue(false);
        }
    }

    sql """insert into alter_s3_vault_tbl values("2", "2"); """

    // rename + aksk
    vaultName = newVaultName
    newVaultName = vaultName + "_new";

    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="S3",
            "VAULT_NAME" = "${newVaultName}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULT;"""
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(newVaultName)) {
            def newProperties = vaultInfos[i][2]
            assert properties == newProperties, "Properties are not the same"
        }
        if (name.equals(vaultName)) {
            assertTrue(false);
        }
    }
    sql """insert into alter_s3_vault_tbl values("2", "2"); """


    vaultName = newVaultName;

    newVaultName = vaultName + "_new";

    vaultInfos = sql """SHOW STORAGE VAULT;"""
    boolean exist = false

    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="S3",
            "VAULT_NAME" = "${newVaultName}",
            "s3.access_key" = "new_ak_ak",
            "s3.secret_key" = "sk"
        );
    """

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(vaultName)) {
            assertTrue(false);
        }
        if (name.equals(newVaultName)) {
            assertTrue(vaultInfos[i][2].contains("new_ak_ak"))
            exist = true
        }
    }
    assertTrue(exist)

    vaultName = newVaultName;

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${vaultName}
            PROPERTIES (
                "type"="S3",
                "VAULT_NAME" = "${dupVaultName}",
                "s3.access_key" = "new_ak_ak",
                "s3.secret_key" = "sk"
            );
        """
    }, "already exists")

    def count = sql """ select count() from alter_s3_vault_tbl; """
    assertTrue(res[0][0] == 4)

    // failed to insert due to the wrong ak
    expectExceptionLike({ sql """insert into alter_s3_vault_tbl values("2", "2");""" }, "")
}
