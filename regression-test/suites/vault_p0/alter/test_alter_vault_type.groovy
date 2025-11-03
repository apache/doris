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

suite("test_alter_vault_type", "nonConcurrent") {
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
    def hdfsVaultName = "hdfs_" + randomStr

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="s3",
                "s3.access_key" = "new_ak",
                "s3.secret_key" = "new_sk"
            );
        """
    }, "is not s3 storage vault")

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
            "use_path_style" = "false",
            "provider" = "${getS3Provider()}"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="hdfs",
                "hadoop.username" = "hdfs"
            );
        """
    }, "is not hdfs storage vault")

    def test_user  = "alter_vault_no_pri";
    def test_pass = "12345"
    sql """create user ${test_user} identified by '${test_pass}'"""

    try {
        def result = connect(test_user, test_pass, context.config.jdbcUrl) {
            sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="s3",
                "VAULT_NAME" = "${s3VaultName}_rename"
            );
        """
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
    }

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}"
        );
    """

    sql """
        ALTER STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT non_existent_vault_${randomStr}
            PROPERTIES (
                "s3.access_key" = "test_ak"
            );
        """
    }, "does not exist")
}