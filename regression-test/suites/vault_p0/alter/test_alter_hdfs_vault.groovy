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

suite("test_alter_hdfs_vault", "nonConcurrent") {
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
    def hdfsVaultName = "hdfs_" + randomStr

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${hdfsVaultName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "${getHmsUser()}"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "path_prefix" = "error_path"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS" = "error_fs"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "s3.endpoint" = "error_endpoint"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "s3.region" = "error_region"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "s3.access_key" = "error_access_key"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type"="hdfs",
                "provider" = "error_provider"
            );
        """
    }, "Alter property")

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
    def result = sql """ select * from ${hdfsVaultName}; """
    assertEquals(result.size(), 1);

    def newHdfsVaultName = hdfsVaultName + "_new";
    sql """
        ALTER STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type"="hdfs",
            "VAULT_NAME" = "${newHdfsVaultName}",
            "hadoop.username" = "hdfs"
        );
    """

    def vaultInfos = sql """ SHOW STORAGE VAULT; """
    boolean found = false
    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("info ${vaultInfos[i]}")
        if (name.equals(hdfsVaultName)) {
            assertTrue(false);
        }
        if (name.equals(newHdfsVaultName)) {
            assertTrue(vaultInfos[i][2].contains("""user: "hdfs" """))
            found = true
        }
    }
    assertTrue(found)

    expectExceptionLike({sql """insert into ${hdfsVaultName} values("2", "2");"""}, "open file failed")
}
