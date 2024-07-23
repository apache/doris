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

suite("create_vault", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip create storgage vault case")
        return
    }

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS failed_vault
            PROPERTIES (
            "type"="S3",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "ssb_sf1_p2",
            "hadoop.username" = "hadoop"
            );
        """
    }, "Missing")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS failed_vault
            PROPERTIES (
            "type"="hdfs",
            "s3.bucket"="${getHmsHdfsFs()}",
            "path_prefix" = "ssb_sf1_p2",
            "hadoop.username" = "hadoop"
            );
        """
    }, "invalid fs_name")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS failed_vault
            PROPERTIES (
            );
        """
    }, "Encountered")


    sql """
        CREATE STORAGE VAULT IF NOT EXISTS create_hdfs_vault
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "default_vault_ssb_hdfs_vault",
        "hadoop.username" = "hadoop"
        );
    """

    try_sql """
        drop table create_table_use_vault
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS create_table_use_vault (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "create_hdfs_vault"
                )
    """

    String create_stmt = sql """
        show create table create_table_use_vault
    """

    logger.info("the create table stmt is ${create_stmt}")
    assertTrue(create_stmt.contains("create_hdfs_vault"))

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT create_hdfs_vault
            PROPERTIES (
            "type"="hdfs",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "default_vault_ssb_hdfs_vault"
            );
        """
    }, "already created")


    sql """
        CREATE STORAGE VAULT IF NOT EXISTS create_s3_vault
        PROPERTIES (
        "type"="S3",
        "s3.endpoint"="${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${getS3AK()}",
        "s3.secret_key" = "${getS3SK()}",
        "s3.root.path" = "ssb_sf1_p2_s3",
        "s3.bucket" = "${getS3BucketName()}",
        "s3.external_endpoint" = "",
        "provider" = "${getS3Provider()}"
        );
    """

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT create_s3_vault
            PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "ssb_sf1_p2_s3",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}"
            );
        """
    }, "already created")

    sql """
        CREATE TABLE IF NOT EXISTS create_table_use_s3_vault (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "create_s3_vault"
                )
    """

    sql """
        insert into create_table_use_s3_vault values(1,1);
    """

    sql """
        select * from create_table_use_s3_vault;
    """


    def vaults_info = try_sql """
        show storage vault
    """

    
    boolean create_hdfs_vault_exist = false;
    boolean create_s3_vault_exist = false;
    boolean built_in_storage_vault_exist = false;
    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        if (name.equals("create_hdfs_vault")) {
            create_hdfs_vault_exist = true;
        }
        if (name.equals("create_s3_vault")) {
            create_s3_vault_exist = true;
        }
        if (name.equals("built_in_storage_vault")) {
            built_in_storage_vault_exist = true
        }
    }
    assertTrue(create_hdfs_vault_exist)
    assertTrue(create_s3_vault_exist)
    assertTrue(built_in_storage_vault_exist)

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS built_in_storage_vault
            PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "ssb_sf1_p2_s3",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}"
            );
        """
    }, "already created")
}
