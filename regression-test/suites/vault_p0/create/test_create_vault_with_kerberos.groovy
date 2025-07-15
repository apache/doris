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

suite("test_create_vault_with_kerberos", "nonConcurrent") {
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
    def hdfsVaultName2 = "hdfs2_" + randomStr
    def tableName = "tbl_" + randomStr
    def tableName2 = "tbl2_" + randomStr

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}
            PROPERTIES (
                "type" = "hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}",
                "hadoop.username" = "not_exist_user"
            );
        """
    }, "Permission denied")

    sql """
        CREATE STORAGE VAULT ${hdfsVaultName}
        PROPERTIES (
            "type" = "hdfs",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName}",
            "hadoop.username" = "not_exist_user",
            "s3_validity_check" = "false"
        );
    """

    sql """
        CREATE TABLE ${tableName} (
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

    expectExceptionLike({
        sql """ insert into ${tableName} values(1, 1); """
    }, "Permission denied: user=not_exist_user")

    expectExceptionLike({
        sql """
            CREATE STORAGE VAULT ${hdfsVaultName}_2
            PROPERTIES (
                "type" = "hdfs",
                "fs.defaultFS"="${getHmsHdfsFs()}",
                "path_prefix" = "${hdfsVaultName}_2",
                "hadoop.username" = "${getHmsUser()}",
                "hadoop.security.authentication" = "kerberos"
            );
        """
    }, "HDFS authentication type is kerberos, but principal or keytab is not set")


    sql """
        CREATE STORAGE VAULT ${hdfsVaultName2}
        PROPERTIES (
            "type" = "hdfs",
            "fs.defaultFS"="${getHmsHdfsFs()}",
            "path_prefix" = "${hdfsVaultName2}",
            "hadoop.username" = "${getHmsUser()}",
            "hadoop.security.authentication" = "kerberos",
            "hadoop.kerberos.principal" = "hadoop/127.0.0.1@XXX",
            "hadoop.kerberos.keytab" = "/etc/not_exist/emr.keytab",
            "s3_validity_check" = "false"
        );
    """

    sql """
        CREATE TABLE ${tableName2} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_vault_name" = ${hdfsVaultName2}
        )
    """

    expectExceptionLike({
        sql """ insert into ${tableName2} values(1, 1); """
    }, "vault id not found, maybe not sync")
}