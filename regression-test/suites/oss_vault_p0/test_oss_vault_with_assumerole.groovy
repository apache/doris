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

suite("test_oss_vault_with_assumerole") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case, because storage vault not enabled")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def ossVaultName = "oss_" + randomStr

    def endpoint = context.config.ossEndpoint
    def region = context.config.ossRegion
    def bucket = context.config.ossBucket
    def roleArn = context.config.ossRoleArn
    def externalId = context.config.ossExternalId
    def prefix = context.config.ossPrefix

    // Test 1: Create OSS storage vault with AssumeRole (no external_id)
    // Note: When role_arn is provided, INSTANCE_PROFILE is automatically used as base credential provider
    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${ossVaultName}
        PROPERTIES (
            "type"="OSS",
            "oss.endpoint"="${endpoint}",
            "oss.region" = "${region}",
            "oss.role_arn" = "${roleArn}",
            "oss.root.path" = "${prefix}/oss_vault_p0/${ossVaultName}",
            "oss.bucket" = "${bucket}"
        );
    """

    // Verify vault was created
    def vaults = sql """ SHOW STORAGE VAULTS """
    boolean vaultFound = false
    for (int i = 0; i < vaults.size(); i++) {
        if (vaults[i][0] == ossVaultName) {
            vaultFound = true
            logger.info("Found OSS vault: ${ossVaultName}")
            break
        }
    }
    assertTrue(vaultFound, "OSS vault should be created")

    // Test 2: Create table using the vault
    sql """
        CREATE TABLE ${ossVaultName}_table (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(100) NOT NULL,
            C_CREATED     DATETIME NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = "${ossVaultName}"
        )
    """

    // Test 3: Insert and query data
    sql """ insert into ${ossVaultName}_table values(1, 'Alice', '2024-01-01 00:00:00'); """
    sql """ insert into ${ossVaultName}_table values(2, 'Bob', '2024-01-02 00:00:00'); """
    sql """ sync;"""

    def result = sql """ select * from ${ossVaultName}_table order by C_CUSTKEY; """
    assertEquals(result.size(), 2)
    assertEquals(result[0][0], 1)
    assertEquals(result[0][1], "Alice")
    assertEquals(result[1][0], 2)
    assertEquals(result[1][1], "Bob")

    // Test 4: Verify data persisted in OSS by reading again
    result = sql """ select count(*) from ${ossVaultName}_table; """
    assertEquals(result[0][0], 2)

    // Test 5: Create another vault with external_id (if configured)
    if (!Strings.isNullOrEmpty(externalId)) {
        def ossVaultNameExtId = "oss_extid_" + randomStr

        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${ossVaultNameExtId}
            PROPERTIES (
                "type"="OSS",
                "oss.endpoint"="${endpoint}",
                "oss.region" = "${region}",
                "oss.role_arn" = "${roleArn}",
                "oss.external_id" = "${externalId}",
                "oss.root.path" = "${prefix}/oss_vault_p0/${ossVaultNameExtId}",
                "oss.bucket" = "${bucket}"
            );
        """

        // Create table with external_id vault
        sql """
            CREATE TABLE ${ossVaultNameExtId}_table (
                ID INTEGER NOT NULL,
                VALUE VARCHAR(100)
            )
            DUPLICATE KEY(ID)
            DISTRIBUTED BY HASH(ID) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "${ossVaultNameExtId}"
            )
        """

        sql """ insert into ${ossVaultNameExtId}_table values(100, 'test_external_id'); """
        sql """ sync;"""

        result = sql """ select * from ${ossVaultNameExtId}_table; """
        assertEquals(result.size(), 1)
        assertEquals(result[0][1], "test_external_id")

        logger.info("Successfully tested OSS vault with external_id")
    }

    logger.info("Successfully completed OSS AssumeRole vault tests")
}
