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

suite("test_create_s3_vault_path_version", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case, because storage vault not enabled")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def s3VaultName = "s3_path_v1_" + randomStr

    def createS3VaultSql = { String vaultName, String pathFormat ->
        return """
            CREATE STORAGE VAULT ${vaultName}
            PROPERTIES (
                "type"="S3",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.root.path" = "${vaultName}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "use_path_style" = "false",
                ${pathFormat}
            );
        """
    }

    // The other invalid path formats are covered by CreateStorageVaultCommandTest
    test {
        sql createS3VaultSql(s3VaultName + "_invalid", """ "path_version" = "1" """)
        exception "shard_num is required when path_version is 1"
    }

    sql createS3VaultSql(s3VaultName, """ "path_version" = "1", "shard_num" = "16" """)

    sql """ DROP TABLE IF EXISTS test_create_s3_vault_path_version_tbl FORCE """
    sql """
        CREATE TABLE test_create_s3_vault_path_version_tbl (
            k1 INT NOT NULL,
            v1 VARCHAR(32) NOT NULL,
            INDEX idx_v1 (v1) USING INVERTED
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = "${s3VaultName}"
        )
    """

    // Every load creates new rowsets whose first segment is 0.dat under path version 1,
    // they must not share file cache entries.
    sql """ INSERT INTO test_create_s3_vault_path_version_tbl VALUES (1, "a"), (2, "b"), (3, "c") """
    sql """ INSERT INTO test_create_s3_vault_path_version_tbl VALUES (4, "d"), (5, "e"), (6, "f") """
    order_qt_select_after_two_loads """ SELECT * FROM test_create_s3_vault_path_version_tbl """
    order_qt_select_by_inverted_index """
        SELECT * FROM test_create_s3_vault_path_version_tbl WHERE v1 MATCH_ANY "e"
    """

    sql """ TRUNCATE TABLE test_create_s3_vault_path_version_tbl """
    sql """ INSERT INTO test_create_s3_vault_path_version_tbl VALUES (7, "g"), (8, "h") """
    order_qt_select_after_truncate """ SELECT * FROM test_create_s3_vault_path_version_tbl """
}
