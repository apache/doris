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

suite("test_show_create_storage_vault_command") {
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
    def s3VaultName = "s3show_" + randomStr
    def hdfsVaultName = "hdfsshow_" + randomStr

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "doris_vault_test",
        "hadoop.username" = "${getHmsUser()}"
        );
    """

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
            "use_path_style" = "false"
        );
    """

    def vaultInfos = try_sql """SHOW STORAGE VAULTS"""
    boolean hdfsVaultExist = false;
    boolean s3VaultExist = false;


    for (int i = 0; i < vaultInfos.size(); i++) {
        logger.info("vault info: ${vaultInfos[i]}")
        if (vaultInfos[i][0].equals(hdfsVaultName)) {
            hdfsVaultExist = true
        }

        if (vaultInfos[i][0].equals(s3VaultName)) {
            s3VaultExist = true
        }
    }
  
    assertTrue(hdfsVaultExist)
    assertTrue(s3VaultExist)

    hdfsVaultExist = false
    s3VaultExist = false
    
    def hdfsVaultInfo = try_sql """SHOW CREATE STORAGE VAULT ${hdfsVaultName}"""
    def s3VaultInfo = try_sql """SHOW CREATE STORAGE VAULT ${s3VaultName}"""

    if (hdfsVaultInfo.size == 1) {
        hdfsVaultExist = true
    }

    if (s3VaultInfo.size == 1) {
        s3VaultExist = true
    }

    assertTrue(hdfsVaultExist)
    assertTrue(s3VaultExist)


}

