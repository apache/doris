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

suite("test_create_vault_with_case_sensitive", "nonConcurrent") {
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
    def hdfsVaultName = "hdfs_" + randomStr

    // hdfs vault case
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "aaaa",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}"
            );
        """
    }, "Unsupported Storage Vault type")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "s3",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}"
            );
        """
    }, "Missing [s3.endpoint] in properties")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "S3",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}"
            );
        """
    }, "Missing [s3.endpoint] in properties")

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type" = "hdfs",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName.toUpperCase()}
        PROPERTIES (
            "TYPE" = "HDFS",
            "FS.DEFAULTFS"="${getHmsHdfsFs()}",
            "PATH_PREFIX" = "${hdfsVaultName.toUpperCase()}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    // s3 vault case
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "bbbb",
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
    }, "Unsupported Storage Vault type")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "hdfs",
                "FS.DEFAULTFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}",
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
    }, "Invalid argument s3.region")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type" = "HDFS",
                "FS.DEFAULTFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}",
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
    }, "Invalid argument s3.region")

    sql """
        CREATE STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type" = "s3",
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

    // S3.xx properties is case sensitive
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName.toUpperCase()}
            PROPERTIES (
                "TYPE" = "S3",
                "S3.ENDPOINT"="${getS3Endpoint()}",
                "S3.REGION" = "${getS3Region()}",
                "S3.ACCESS_KEY" = "${getS3AK()}",
                "S3.SECRET_KEY" = "${getS3SK()}",
                "S3.ROOT.PATH" = "${s3VaultName}",
                "S3.BUCKET" = "${getS3BucketName()}",
                "S3.EXTERNAL_ENDPOINT" = "",
                "PROVIDER" = "${getS3Provider()}",
                "USE_PATH_STYLE" = "false"
            );
        """
    }, "Missing [s3.endpoint] in properties")

    sql """
        CREATE STORAGE VAULT ${s3VaultName.toUpperCase()}
        PROPERTIES (
            "TYPE" = "S3",
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

    def vaultInfos = try_sql """SHOW STORAGE VAULTS"""

    boolean hdfsVaultLowerExist = false;
    boolean hdfsVaultUpperExist = false;

    boolean s3VaultLowerExist = false;
    boolean s3VaultUpperExist = false;

    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(hdfsVaultName)) {
            hdfsVaultLowerExist = true
        }

        if (vaultInfos[i][0].equals(hdfsVaultName.toUpperCase())) {
            hdfsVaultUpperExist = true
        }

        if (vaultInfos[i][0].equals(s3VaultName)) {
            s3VaultLowerExist = true
        }

        if (vaultInfos[i][0].equals(s3VaultName.toUpperCase())) {
            s3VaultUpperExist = true
        }
    }
    assertTrue(hdfsVaultLowerExist)
    assertTrue(hdfsVaultUpperExist)
    assertTrue(s3VaultLowerExist)
    assertTrue(s3VaultUpperExist)
}