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

suite("test_default_vault", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case")
        return
    }

    try {
        sql """ UNSET DEFAULT STORAGE VAULT; """

        expectExceptionLike({
            sql """ set not_exist as default storage vault """
        }, "invalid storage vault name")

        def tableName = "table_use_vault"
        sql "DROP TABLE IF EXISTS ${tableName}"

        expectExceptionLike({
            sql """
                CREATE TABLE ${tableName} (
                    `key` INT,
                    value INT
                ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
            """
        }, "No default storage vault")

        sql """
            CREATE STORAGE VAULT IF NOT EXISTS create_s3_vault_for_default
            PROPERTIES (
                "type"="S3",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.root.path" = "create_s3_vault_for_default",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "set_as_default" = "true"
            );
        """

        sql """ set create_s3_vault_for_default as default storage vault """
        def vaultInfos = sql """ SHOW STORAGE VAULT """
        // check if create_s3_vault_for_default is set as default
        for (int i = 0; i < vaultInfos.size(); i++) {
            def name = vaultInfos[i][0]
            if (name.equals("create_s3_vault_for_default")) {
                // isDefault is true
                assertEquals(vaultInfos[i][3], "true")
            }
        }

        sql """ UNSET DEFAULT STORAGE VAULT; """
        vaultInfos = sql """ SHOW STORAGE VAULT """
        for (int i = 0; i < vaultInfos.size(); i++) {
            assertEquals(vaultInfos[i][3], "false")
        }


        sql """ set built_in_storage_vault as default storage vault """

        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """

        sql """ insert into ${tableName} values(1, 1); """
        sql """ sync;"""
        def result = sql """ select * from ${tableName}; """
        logger.info("result:${result}");
        assertTrue(result.size() == 1)
        assertTrue(result[0][0].toInteger() == 1)

        def create_table_stmt = sql """ show create table ${tableName} """
        assertTrue(create_table_stmt[0][1].contains("built_in_storage_vault"))

        sql """
            CREATE STORAGE VAULT IF NOT EXISTS create_default_hdfs_vault
            PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "default_vault_ssb_hdfs_vault",
                "hadoop.username" = "hadoop"
            );
        """

        sql """ set create_default_hdfs_vault as default storage vault """

        sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """

        create_table_stmt = sql """ show create table ${tableName} """
        assertTrue(create_table_stmt[0][1].contains("create_default_hdfs_vault"))

        expectExceptionLike({
            sql """
                alter table ${tableName} set("storage_vault_name" = "built_in_storage_vault");
            """
        }, "You can not modify")

    } finally {
        sql """ set built_in_storage_vault as default storage vault """
        sql """ set built_in_storage_vault as default storage vault """
    }
}
