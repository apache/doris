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

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def s3VaultName = "s3_" + randomStr

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
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

    // case1
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "fs.defaultFS"="error_fs"
            );
        """
    }, "Alter property")

    // case2
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "path_prefix"="error_path"
            );
        """
    }, "Alter property")

    // case3
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "hadoop.username"="error_user"
            );
        """
    }, "Alter property")

    // case4
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.bucket" = "error_bucket"
            );
        """
    }, "Alter property")

    // case5
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.region" = "error_region"
            );
        """
    }, "Alter property")

    // case6
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.endpoint" = "error_endpoint"
            );
        """
    }, "Alter property")

    // case7
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "provider" = "error_privider"
            );
        """
    }, "Alter property")

    // case8
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "provider" = "error_privider"
            );
        """
    }, "Alter property")

    // case9
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.root.path" = "error_root_path"
            );
        """
    }, "Alter property")

    // case10
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.external_endpoint" = "error_external_endpoint"
            );
        """
    }, "Alter property")

    // case11
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.access_key" = "new_ak"
            );
        """
    }, "Accesskey and secretkey must be alter together")

    // case12
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.access_key" = "new_ak"
            );
        """
    }, "Accesskey and secretkey must be alter together")

    // case13
    def String properties;
    def vaultInfos = try_sql """SHOW STORAGE VAULTS"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(s3VaultName)) {
            properties = vaultInfos[i][2]
        }
    }

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
    """

    vaultInfos = sql """SHOW STORAGE VAULTS;"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(s3VaultName)) {
            // [s3_9f2c2f80a45c4fb48ad901f70675ffdf, 8, ctime: 1750217403 mtime: 1750217404 ak: "*******" sk: "xxxxxxx" bucket: "xxx" ...]
            def newProperties = vaultInfos[i][2]
            def oldAkSkStr = properties.substring(properties.indexOf("ak:") + 3, properties.indexOf("bucket:") - 1)
            def newAkSkStr = newProperties.substring(newProperties.indexOf("ak:") + 3, newProperties.indexOf("bucket:") - 1)
            assertTrue(oldAkSkStr.equals(newAkSkStr), "Ak and Sk string are not the same")
        }
    }


    // case14 rename + aksk
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

    def newS3VaultName = s3VaultName + "_new";
    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "VAULT_NAME" = "${newS3VaultName}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
        """

    sql """ insert into ${s3VaultName} values(2, 2); """
    sql """ sync;"""
    result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 2);

    expectExceptionLike({
        sql """
            CREATE TABLE ${newS3VaultName} (
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

    sql """
        CREATE TABLE ${newS3VaultName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${newS3VaultName}
        )
    """

    sql """ insert into ${newS3VaultName} values(1, 1); """
    sql """ sync;"""
    result = sql """ select * from ${newS3VaultName}; """
    assertEquals(result.size(), 1);

    // case15
    sql """
        ALTER STORAGE VAULT ${newS3VaultName}
        PROPERTIES (
            "type"="S3",
            "s3.access_key" = "error_ak",
            "s3.secret_key" = "error_sk"
        );
    """
    expectExceptionLike({ sql """insert into ${newS3VaultName} values("2", "2");""" }, "failed to put object")
}
