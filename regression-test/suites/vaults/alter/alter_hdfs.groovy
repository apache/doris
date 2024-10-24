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

suite("alter_hdfs_vault", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip alter hdfs storgage vault case")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS alter_hdfs_vault
        PROPERTIES (
        "type"="HDFS",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "ssb_sf1_p2",
        "hadoop.username" = "hadoop"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS alter_hdfs_vault_tbl (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "alter_hdfs_vault"
                )
    """

    sql """
        insert into alter_hdfs_vault_tbl values("1", "1");
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT alter_hdfs_vault
            PROPERTIES (
            "type"="hdfs",
            "path_prefix" = "ssb_sf1_p3"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT alter_hdfs_vault
            PROPERTIES (
            "type"="hdfs",
            "fs.defaultFS" = "ssb_sf1_p3"
            );
        """
    }, "Alter property")

    def vault_name = "alter_hdfs_vault"
    String properties;

    def vaults_info = try_sql """
        show storage vault
    """

    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        if (name.equals(vault_name)) {
            properties = vaults_info[i][2]
        }
    }
    
    sql """
        ALTER STORAGE VAULT alter_hdfs_vault
        PROPERTIES (
        "type"="hdfs",
        "VAULT_NAME" = "alter_hdfs_vault_new_name",
        "hadoop.username" = "hdfs"
        );
    """

    def new_vault_name = "alter_hdfs_vault_new_name"

    vaults_info = sql """
        SHOW STORAGE VAULT;
    """
    boolean exist = false

    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        logger.info("name is ${name}, info ${vaults_info[i]}")
        if (name.equals(vault_name)) {
            exist = true
        }
        if (name.equals(new_vault_name)) {
            assertTrue(vaults_info[i][2].contains(""""hadoop.username" = "hdfs"""""))
        }
    }
    assertFalse(exist)

    // failed to insert due to the wrong ak
    expectExceptionLike({
        sql """
            insert into alter_hdfs_vault_tbl values("2", "2");
        """
    }, "")
}
