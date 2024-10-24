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

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${suiteName}
        PROPERTIES (
            "type"="HDFS",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${suiteName}",
            "hadoop.username" = "hadoop"
        );
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
                "type"="hdfs",
                "path_prefix" = "${suiteName}"
            );
        """
    }, "Alter property")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
                "type"="hdfs",
                "fs.defaultFS" = "not_exist_vault"
            );
        """
    }, "Alter property")

    def vaultName = suiteName
    String properties;

    def vaultInfos = try_sql """show storage vault"""

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        if (name.equals(vaultName)) {
            properties = vaultInfos[i][2]
        }
    }

    def newVaultName = suiteName + "_new";
    sql """
        ALTER STORAGE VAULT ${vaultName}
        PROPERTIES (
            "type"="hdfs",
            "VAULT_NAME" = "${newVaultName}",
            "hadoop.username" = "hdfs"
        );
    """

    vaultInfos = sql """ SHOW STORAGE VAULT; """
    boolean exist = false

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(vaultName)) {
            assertTrue(false);
        }
        if (name.equals(newVaultName)) {
            assertTrue(vaultInfos[i][2].contains("""user: "hdfs" """))
            exist = true
        }
    }
    assertTrue(exist)
    expectExceptionLike({sql """insert into ${suiteName} values("2", "2");"""}, "")
}
