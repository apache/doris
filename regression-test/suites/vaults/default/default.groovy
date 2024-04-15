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

suite("default_vault") {
    if (enableStoragevault()) {
        return
    }
    try {
        sql """
            set not_exist as default vault
        """
    } catch (Exception e) {
    }

    def tableName = "table_use_vault"

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    } catch (Exception e) {
        assertTrue(e.getMessage().contains('supply'))
    }

    sql """
        set built_in_storage_vault as default vault
    """

    {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    }

    sql """
        set built_in_storage_vault as default vault
    """

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS create_default_hdfs_vault
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHdfsFs()}",
        "root_prefix" = "default_vault_ssb_hdfs_vault"
        );
    """

    sql """
        set create_default_hdfs_vault as default vault
    """

    {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
        sql """
            insert into ${tableName} vaules(1. 1);
        """
        sql """
            select * from ${tableName};
        """

        def create_table_stmt = """
            show create table ${tableName}
        """

        assertTrue(create_table_stmt.contains("create_default_hdfs_vault"))

        try {
            sql """
                alter table ${tableName} set("storage_vault_name" = "built_in_storage_vault");
            """
        } catch (Exception e) {
        }
    }

    try {
        sql """
            set @ as default vault
        """
    } catch (Exception e) {
    }

    try {
        sql """
            set % as default vault
        """
    } catch (Exception e) {
    }

    try {
        sql """
            set null as default vault
        """
    } catch (Exception e) {
    }

    try {
        sql """
            set $ as default vault
        """
    } catch (Exception e) {
    }
}