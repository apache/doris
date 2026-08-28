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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_s3_express_storage_vault_restart_docker", "p0,external,docker") {
    String enabled = context.config.otherConfigs.get("enableS3ExpressStorageVaultTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
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
    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableStorageVault = true
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.recyclerNum = 1
    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'heartbeat_interval_second=1'
    ]
    options.cloudStoreConfigs += [
            'DORIS_CLOUD_USER=s3express-regression',
            "DORIS_CLOUD_AK=${accessKey}",
            "DORIS_CLOUD_SK=${secretKey}",
            "DORIS_CLOUD_BUCKET=${bucket}",
            "DORIS_CLOUD_ENDPOINT=s3.${region}.amazonaws.com",
            "DORIS_CLOUD_EXTERNAL_ENDPOINT=s3.${region}.amazonaws.com",
            "DORIS_CLOUD_REGION=${region}",
            'DORIS_CLOUD_PROVIDER=S3EXPRESS'
    ]

    docker(options) {
        String suffix = UUID.randomUUID().toString().replace("-", "")
        String vaultName = "s3_express_restart_${suffix}"
        String rootPath = "${prefix}/${vaultName}"

        sql """DROP TABLE IF EXISTS test_s3_express_storage_vault_restart"""
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
        sql """
            CREATE TABLE test_s3_express_storage_vault_restart (
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
            INSERT INTO test_s3_express_storage_vault_restart
            VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')
        """
        sql """SYNC"""
        order_qt_s3_express_storage_vault_before_restart """
            SELECT k, v
            FROM test_s3_express_storage_vault_restart
            ORDER BY k
        """

        // FoundationDB remains running as the durable metadata store while every Doris service
        // that consumes Storage Vault metadata is restarted through the Docker framework.
        cluster.restartCloudServices()
        context.reconnectFe()

        boolean backendAlive = false
        for (int retry = 0; retry < 60; retry++) {
            def backends = sql_return_maparray("SHOW BACKENDS")
            backendAlive = !backends.isEmpty() && backends.every { it.Alive.toBoolean() }
            if (backendAlive) {
                break
            }
            sleep(1000)
        }
        if (!backendAlive) {
            throw new IllegalStateException("Backend did not become alive after the full cluster restart")
        }

        order_qt_s3_express_storage_vault_after_restart """
            SELECT k, v
            FROM test_s3_express_storage_vault_restart
            ORDER BY k
        """

        sql """
            INSERT INTO test_s3_express_storage_vault_restart
            VALUES (4, 'delta')
        """
        sql """SYNC"""
        order_qt_s3_express_storage_vault_write_after_restart """
            SELECT k, v
            FROM test_s3_express_storage_vault_restart
            ORDER BY k
        """
    }
}
