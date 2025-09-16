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

suite("test_default_vault", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    expectExceptionLike({
        sql """ set not_exist as default storage vault"""
    }, "invalid storage vault name")

    expectExceptionLike({
        sql """ set null as default storage vault"""
    }, "no viable alternative at input")

    sql """ UNSET DEFAULT STORAGE VAULT; """
    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def s3VaultName = "s3_" + randomStr
    def s3TableName = "s3_tbl_" + randomStr
    def hdfsVaultName = "hdfs_" + randomStr
    def hdfsTableName = "hdfs_tbl_" + randomStr

    expectExceptionLike({
        sql """
            CREATE TABLE ${s3VaultName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    }, "No default storage vault")

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
            "use_path_style" = "false",
            "set_as_default" = "true"
        );
    """

    boolean found = false
    def vaultInfos = sql """ SHOW STORAGE VAULTS """
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        if (name.equals(s3VaultName)) {
            // isDefault is true
            assertEquals(vaultInfos[i][3], "true")
            found = true
            break;
        }
    }
    assertTrue(found)

    sql """
        CREATE TABLE ${s3TableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ insert into ${s3TableName} values(1, 1); """
    sql """ sync;"""
    def result = sql """ select * from ${s3TableName}; """
    assertEquals(result.size(), 1);
    def createTableStmt = sql """ show create table ${s3TableName} """
    assertTrue(createTableStmt[0][1].contains(s3VaultName))

    expectExceptionLike({
        sql """
            alter table ${s3TableName} set("storage_vault_name" = "not_exist_vault");
        """
    }, "You can not modify")

    sql """ UNSET DEFAULT STORAGE VAULT; """
    vaultInfos = sql """ SHOW STORAGE VAULT """
    for (int i = 0; i < vaultInfos.size(); i++) {
        assertEquals(vaultInfos[i][3], "false")
    }

    expectExceptionLike({
        sql """
            CREATE TABLE ${s3TableName}_2 (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    }, "No default storage vault")

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}",
            "set_as_default" = "true"
        );
    """

    found = false
    vaultInfos = sql """ SHOW STORAGE VAULTS """
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        if (name.equals(hdfsVaultName)) {
            // isDefault is true
            assertEquals(vaultInfos[i][3], "true")
            found = true
            break;
        }
    }
    assertTrue(found)

    sql """
        CREATE TABLE ${hdfsTableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ insert into ${hdfsTableName} values(1, 1); """
    sql """ sync;"""
    result = sql """ select * from ${hdfsTableName}; """
    assertEquals(result.size(), 1);
    createTableStmt = sql """ show create table ${hdfsTableName} """
    assertTrue(createTableStmt[0][1].contains(hdfsVaultName))

    expectExceptionLike({
        sql """
            alter table ${hdfsTableName} set("storage_vault_name" = "${hdfsVaultName}");
        """
    }, "You can not modify")

    // test set stmt
    sql """SET ${s3VaultName} AS DEFAULT STORAGE VAULT;"""
    found = false
    vaultInfos = sql """ SHOW STORAGE VAULTS """
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        if (name.equals(s3VaultName)) {
            // isDefault is true
            assertEquals(vaultInfos[i][3], "true")
            found = true
            break;
        }
    }
    assertTrue(found)

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="s3",
                "VAULT_NAME" = "${s3VaultName}_rename"
            );
        """
    }, "Cannot rename default storage vault")

    sql """
        CREATE TABLE ${s3TableName}_2 (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ insert into ${s3TableName}_2 values(1, 1); """
    sql """ sync;"""
    result = sql """ select * from ${s3TableName}_2; """
    assertEquals(result.size(), 1);
    createTableStmt = sql """ show create table ${s3TableName}_2 """
    assertTrue(createTableStmt[0][1].contains(s3VaultName))

    sql """ UNSET DEFAULT STORAGE VAULT; """
    vaultInfos = sql """ SHOW STORAGE VAULTS """
    for (int i = 0; i < vaultInfos.size(); i++) {
        if(vaultInfos[i][3].equalsIgnoreCase"true") {
            assertFalse(true)
        }
    }
}
