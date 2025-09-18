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

suite("test_alter_s3_vault_with_role") {
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

    def endpoint = context.config.awsEndpoint
    def region = context.config.awsRegion
    def bucket = context.config.awsBucket
    def roleArn = context.config.awsRoleArn
    def externalId = context.config.awsExternalId
    def prefix = context.config.awsPrefix
    def awsAccessKey = context.config.awsAccessKey
    def awsSecretKey = context.config.awsSecretKey

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${endpoint}",
            "s3.region" = "${region}",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id" = "${externalId}",
            "s3.root.path" = "${prefix}/aws_iam_role_p0/${s3VaultName}",
            "s3.bucket" = "${bucket}",
            "s3.external_endpoint" = "",
            "provider" = "S3",
            "use_path_style" = "false"
        );
    """

    sql """
        CREATE TABLE ${s3VaultName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${s3VaultName}
        )
    """
    sql """ insert into ${s3VaultName} values(1, 1); """
    sql """ sync;"""
    def result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 1);

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.access_key" = "${awsAccessKey}",
            "s3.secret_key" = "${awsSecretKey}"
        );
    """

    def vaultInfos = sql """SHOW STORAGE VAULTS;"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(s3VaultName)) {
            def newProperties = vaultInfos[i][2]
            logger.info("newProperties: ${newProperties}")
            assertTrue(newProperties.contains(awsAccessKey))
            assertFalse(newProperties.contains("role_arn"))
        }
    }

    sql """ insert into ${s3VaultName} values(2, 2); """
    sql """ sync;"""
    result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 2);

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id" = "${externalId}"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULTS;"""
    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(s3VaultName)) {
            def newProperties = vaultInfos[i][2]
            logger.info("newProperties: ${newProperties}")
            assertFalse(newProperties.contains(awsAccessKey))
            assertTrue(newProperties.contains(roleArn))
        }
    }

    sql """ insert into ${s3VaultName} values(3, 3); """
    sql """ sync;"""
    result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 3);
}