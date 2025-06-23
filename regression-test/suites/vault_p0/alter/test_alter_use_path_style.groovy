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

suite("test_alter_use_path_style", "nonConcurrent") {
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

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${s3VaultName}
            PROPERTIES (
            "type"="S3",
            "use_path_style" = ""
            );
        """
    }, "use_path_style cannot be empty")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "use_path_style" = "abc"
            );
        """
    }, "Invalid use_path_style value")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "use_path_style" = "@#¥%*&-+=null."
            );
        """
    }, "Invalid use_path_style value")

    sql """
        CREATE TABLE IF NOT EXISTS ${s3VaultName}
        (
            `k1` INT NULL,
            `v1` INT NULL
        )
        UNIQUE KEY (k1)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "storage_vault_name" = "${s3VaultName}"
        );
    """

    sql """ insert into ${s3VaultName} values(1, 1); """
    sql """ sync;"""
    def result = sql """ select * from ${s3VaultName}; """
    assertEquals(result.size(), 1);

    sql """
        ALTER STORAGE VAULT ${s3VaultName}
        PROPERTIES (
            "type"="S3",
            "use_path_style" = "true"
        );
    """

    def vaultInfos = sql """ SHOW STORAGE VAULT; """
    boolean found = false

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("info ${vaultInfos[i]}")
        if (name.equals(s3VaultName)) {
            assertTrue(vaultInfos[i][2].contains("""use_path_style: true"""))
            found = true
            break
        }
    }
    assertTrue(found)

    if ("OSS".equalsIgnoreCase(getS3Provider().trim())) {
        // OSS public cloud not allow url path style
        expectExceptionLike({ sql """insert into ${s3VaultName} values("2", "2");""" }, "failed to put object")
    } else {
        sql """ insert into ${s3VaultName} values(2, 2); """
        sql """ sync;"""
        result = sql """ select * from ${s3VaultName}; """
        assertEquals(result.size(), 2);
    }
}