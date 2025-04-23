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

import java.util.stream.Collectors;
import java.util.stream.Stream;

suite("test_create_vault", "nonConcurrent") {
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

    def length64Str = Stream.generate(() -> String.valueOf('a'))
                     .limit(32) 
                     .collect(Collectors.joining()) + randomStr

    def exceed64LengthStr = length64Str + "a"

    // test long length storage vault
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${exceed64LengthStr}
            PROPERTIES (
                "type"="S3",
                "fs.defaultFS"="hdfs://127.0.0.1:8090",
                "path_prefix" = "${exceed64LengthStr}"
            );
           """
    }, "Incorrect vault name")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT '@#¥%*&-+=null.'
            PROPERTIES (
                "type"="S3"
            );
           """
    }, "Incorrect vault name")

    sql """
        CREATE STORAGE VAULT ${length64Str}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${length64Str}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${s3VaultName}"
            );
           """
    }, "Missing [s3.endpoint] in properties")


    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${s3VaultName}"
            );
           """
    }, "Missing [s3.endpoint] in properties")

    expectExceptionLike({
        sql """ CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName} PROPERTIES (); """
    }, "mismatched input ')'")


    expectExceptionLike({
        sql """
            CREATE TABLE ${s3VaultName} (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "not_exist_vault"
            )
        """
    }, "Storage vault 'not_exist_vault' does not exist")

    // test s3.root.path cannot be empty
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.root.path" = "",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "use_path_style" = "false"
            );
        """
    }, "Property s3.root.path cannot be empty")
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="S3",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "use_path_style" = "false"
            );
        """
    }, "Missing property s3.root.path")

    // missing property type
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "use_path_style" = "false"
            );
        """
    }, "Missing property type")
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${s3VaultName}
            PROPERTIES (
                "type"="",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "use_path_style" = "false"
            );
        """
    }, "Property type cannot be empty")

    // test `if not exist` and dup name s3 vault
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

    expectExceptionLike({
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
    }, "already created")

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

    // hdfs vault case
    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "s3.bucket"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "hadoop.username" = "${getHmsUser()}"
            );
            """
    }, "Invalid argument s3.bucket")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}",
                "s3_validity_check" = "false"
            );
            """
    }, "invalid fs_name")

    // test `if not exist` and dup name hdfs vault
    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="HDFS",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "${getHmsUser()}"
            );
        """
    }, "already created")

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

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
    result = sql """ select * from ${hdfsVaultName}; """
    assertEquals(result.size(), 1);

    boolean hdfsVaultExisted = false;
    boolean s3VaultExisted = false;
    def vaults_info = try_sql """ SHOW STORAGE VAULTS """

    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        if (name.equals(hdfsVaultName)) {
            hdfsVaultExisted = true;
        }
        if (name.equals(s3VaultName)) {
            s3VaultExisted = true;
        }
    }
    assertTrue(hdfsVaultExisted)
    assertTrue(s3VaultExisted)
}
