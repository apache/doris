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

suite("test_alter_vault_name", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    def vaultName = UUID.randomUUID().toString().replace("-", "")
    def hdfsVaultName = "hdfs_" + vaultName
    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type" = "HDFS",
            "fs.defaultFS" = "${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    def s3VaultName = "s3_" + vaultName
    sql """
        CREATE STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type" = "S3",
            "s3.endpoint" = "${getS3Endpoint()}",
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

    // case1
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "hdfs",
                "VAULT_NAME" = "${hdfsVaultName}"
            );
        """
    }, "Vault name has not been changed")

    // case2
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "hdfs",
                "VAULT_NAME" = "${s3VaultName}"
            );
        """
    }, "already existed")

    // case3
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "s3",
                "VAULT_NAME" = "${s3VaultName}"
            );
        """
    }, "Vault name has not been changed")

    // case4
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "s3",
                "VAULT_NAME" = "${hdfsVaultName}"
            );
        """
    }, "already existed")

    // case5
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "s3",
                "VAULT_NAME" = "@#¥%*&-+=null."
            );
        """
    }, "Incorrect vault name")

    // case6
    sql """
        CREATE TABLE ${hdfsVaultName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${hdfsVaultName}
        )
    """
    sql """ insert into ${hdfsVaultName} values(1, 1); """
    sql """ sync;"""
    def result = sql """ select * from ${hdfsVaultName}; """
    assertEquals(result.size(), 1);

    sql """
        ALTER STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type" = "hdfs",
            "VAULT_NAME" = "${hdfsVaultName}_new"
        );
        """
    sql """ insert into ${hdfsVaultName} values(2, 2); """
    sql """ sync;"""
    result = sql """ select * from ${hdfsVaultName}; """
    assertEquals(result.size(), 2);

    // case6
    expectExceptionLike({
        sql """
            CREATE TABLE ${hdfsVaultName}_new (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = ${hdfsVaultName}
            )
        """
    }, "does not exis")

    // case7
    sql """
        CREATE TABLE ${hdfsVaultName}_new (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${hdfsVaultName}_new
        )
    """

    sql """ insert into ${hdfsVaultName}_new values(1, 1); """
    sql """ sync;"""
    result = sql """ select * from ${hdfsVaultName}_new; """
    assertEquals(result.size(), 1);

    // case8
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
    result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 1);

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type" = "s3",
            "VAULT_NAME" = "${s3VaultName}_new"
        );
        """
    sql """ insert into ${s3VaultName} values(2, 2); """
    sql """ sync;"""
    result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 2);

    // case9
    expectExceptionLike({
        sql """
            CREATE TABLE ${s3VaultName}_new (
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
    }, "does not exis")

    // case10
    sql """
        CREATE TABLE ${s3VaultName}_new (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${s3VaultName}_new
        )
    """

    sql """ insert into ${s3VaultName}_new values(1, 1); """
    sql """ sync;"""
    result = sql """ select * from ${s3VaultName}_new; """
    assertEquals(result.size(), 1);
}