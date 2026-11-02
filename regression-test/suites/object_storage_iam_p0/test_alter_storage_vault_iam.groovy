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

suite("test_alter_storage_vault_iam") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case, because storage vault not enabled")
        return
    }
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def s3VaultName = "object_storage_iam_" + randomStr
    def firstAuthCase = config.authCases[0]
    def awsAccessKey = context.config.awsAccessKey
    def awsSecretKey = context.config.awsSecretKey

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            ${firstAuthCase.storageSqlProperties},
            "s3.root.path" = "${config.prefix}/test_alter_storage_vault_iam/${s3VaultName}",
            "s3.bucket" = "${config.bucket}",
            "s3.external_endpoint" = "",
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

    def assertVaultProperties = { Map<String, String> expected, List<String> unexpected ->
        def vaultInfos = sql "SHOW STORAGE VAULTS"
        def vaultInfo = vaultInfos.find { it[0].equals(s3VaultName) }
        assertTrue(vaultInfo != null, "storage vault ${s3VaultName} was not found")
        def newProperties = vaultInfo[2]
        logger.info("newProperties: ${newProperties}")
        expected.each { key, value ->
            assertTrue(newProperties.contains(value), "${key} was not updated")
        }
        unexpected.findAll { it != null && !it.isEmpty() }.each { value ->
            assertFalse(newProperties.contains(value), "stale credential property was not removed")
        }
    }

    def insertAndAssert = { int rowCount ->
        sql "insert into ${s3VaultName} values(${rowCount}, ${rowCount})"
        sql "sync"
        result = sql "select * from ${s3VaultName}"
        assertEquals(result.size(), rowCount)
    }

    if (config.provider == "AWS") {
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.access_key" = "${awsAccessKey}",
                "s3.secret_key" = "${awsSecretKey}"
            );
        """
        assertVaultProperties(["s3.access_key": awsAccessKey], [config.roleArn])
        insertAndAssert(2)

        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                ${firstAuthCase.authProperties}
            );
        """
        assertVaultProperties(firstAuthCase.properties, [awsAccessKey])
        insertAndAssert(3)
    } else {
        def impersonationAccounts = config.authCases.collect {
            it.properties.get("gs.impersonation_service_account")
        }.findAll { it != null }
        // Omitted authentication is a CREATE default, not a request to reset credentials on ALTER.
        // Exercise a storage-only update before switching to the explicitly configured providers.
        sql """
            ALTER STORAGE VAULT ${s3VaultName} PROPERTIES (
                "type" = "S3",
                "use_path_style" = "true"
            )
        """
        assertVaultProperties(firstAuthCase.properties ?: ["gs.credential_provider_type": "DEFAULT"], [])
        insertAndAssert(2)

        def explicitCases = config.authCases.findAll { !it.properties.isEmpty() }
        def transitionCases = explicitCases.isEmpty() ? [] : explicitCases + [explicitCases[0]]
        transitionCases.eachWithIndex { authCase, index ->
            logger.info("alter ${name} to ${authCase.name}")
            def alterProperties = new LinkedHashMap(authCase.properties)
            // A direct-credential transition must explicitly clear the old impersonation target.
            alterProperties.putIfAbsent("gs.impersonation_service_account", "")
            sql """
                ALTER STORAGE VAULT ${s3VaultName}
                PROPERTIES (
                    "type"="S3",
                    ${ObjectStorageIamTestUtils.toSqlProperties(alterProperties)}
                );
            """
            def unexpected = authCase.properties.containsKey("gs.impersonation_service_account")
                    ? [] : impersonationAccounts
            assertVaultProperties(authCase.properties, unexpected)
            insertAndAssert(index * 2 + 3)

            // Unrelated ALTERs must preserve COMPUTE_ENGINE and impersonation as well as DEFAULT.
            sql """
                ALTER STORAGE VAULT ${s3VaultName} PROPERTIES (
                    "type" = "S3",
                    "use_path_style" = "${index % 2 == 0 ? 'false' : 'true'}"
                )
            """
            assertVaultProperties(authCase.properties, unexpected)
            insertAndAssert(index * 2 + 4)
        }
    }
}
