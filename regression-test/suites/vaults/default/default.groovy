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

suite("default_vault", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip create storgage vault case")
        return
    }
    expectExceptionLike({
        sql """
            set not_exist as default storage vault
        """
    }, "invalid storage vault name")

    def tableName = "table_use_vault"

    expectExceptionLike({
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    }, "supply")

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS create_s3_vault_for_default
        PROPERTIES (
        "type"="S3",
        "s3.endpoint"="${getS3Endpoint()}",
        "s3.region" = "${getS3Region()}",
        "s3.access_key" = "${getS3AK()}",
        "s3.secret_key" = "${getS3SK()}",
        "s3.root.path" = "ssb_sf1_p2_s3",
        "s3.bucket" = "${getS3BucketName()}",
        "s3.external_endpoint" = "",
        "provider" = "${getS3Provider()}",
        "set_as_default" = "true"
        );
    """

    def vaults_info = sql """
        show storage vault
    """

    // check if create_s3_vault_for_default is set as default
    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        if (name.equals("create_s3_vault_for_default")) {
            // isDefault is true
            assertEquals(vaults_info[i][3], "true")
        }
    }


    sql """
        set built_in_storage_vault as default storage vault
    """


    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `key` INT,
            value INT
        ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """


    sql """
        set built_in_storage_vault as default storage vault
    """

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS create_default_hdfs_vault
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "default_vault_ssb_hdfs_vault",
        "hadoop.username" = "hadoop"
        );
    """

    sql """
        set create_default_hdfs_vault as default storage vault
    """

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `key` INT,
            value INT
        ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        insert into ${tableName} values(1, 1);
    """
    sql """
        select * from ${tableName};
    """

    def create_table_stmt = sql """
        show create table ${tableName}
    """

    assertTrue(create_table_stmt[0][1].contains("create_default_hdfs_vault"))

    expectExceptionLike({
        sql """
            alter table ${tableName} set("storage_vault_name" = "built_in_storage_vault");
        """
    }, "You can not modify")

    try {
        sql """
            set null as default storage vault
        """
    } catch (Exception e) {
    }

}
