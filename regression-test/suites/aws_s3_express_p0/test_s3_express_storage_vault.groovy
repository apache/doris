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

suite("test_s3_express_storage_vault", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableS3ExpressStorageVaultTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
    }

    if (!isCloudMode()) {
        throw new IllegalStateException("S3 Express Storage Vault test requires cloud mode")
    }
    if (!enableStoragevault()) {
        throw new IllegalStateException("S3 Express Storage Vault test requires storage vault mode")
    }

    def requireConfig = { String key ->
        String value = context.config.otherConfigs.get(key)
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "${key} must be configured when enableS3ExpressStorageVaultTest is true")
        }
        return value
    }

    String bucket = requireConfig("s3ExpressBucketName")
    String region = requireConfig("s3ExpressRegion")
    String prefix = "vault-runs/regression"
    String accessKey = requireConfig("s3ExpressAk")
    String secretKey = requireConfig("s3ExpressSk")

    if (!(bucket ==~ /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--[a-z0-9-]+-az[0-9]+--x-s3$/)) {
        throw new IllegalArgumentException(
                "s3ExpressBucketName must be a complete S3 Express directory bucket name")
    }
    if (!(region ==~ /^[a-z0-9-]+$/)) {
        throw new IllegalArgumentException("s3ExpressRegion contains unsupported characters")
    }
    String suffix = UUID.randomUUID().toString().replace("-", "")
    String vaultName = "s3_express_vault_${suffix}"
    String rootPath = "${prefix}/${vaultName}"

    sql """DROP TABLE IF EXISTS test_s3_express_storage_vault"""

    // CREATE performs the complete S3 validity ping in this order:
    // PUT -> HEAD -> LIST -> multipart upload -> DELETE.
    sql """
        CREATE STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type" = "S3",
            "provider" = "S3EXPRESS",
            "s3.region" = "${region}",
            "s3.bucket" = "${bucket}",
            "s3.root.path" = "${rootPath}",
            "s3.access_key" = "${accessKey}",
            "s3.secret_key" = "${secretKey}",
            "use_path_style" = "false",
            "s3_validity_check" = "true"
        )
    """
    order_qt_s3_express_storage_vault_create_and_ping """
        SELECT 'S3EXPRESS', 'CREATE_AND_PING_OK'
    """

    sql """
        CREATE TABLE test_s3_express_storage_vault (
            k INT,
            v VARCHAR(32)
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = "${vaultName}"
        )
    """
    sql """
        INSERT INTO test_s3_express_storage_vault
        VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')
    """
    sql """SYNC"""
    order_qt_s3_express_storage_vault_insert """
        SELECT k, v
        FROM test_s3_express_storage_vault
        ORDER BY k
    """
}
