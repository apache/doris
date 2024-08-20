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

suite("alter_s3_vault", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip alter s3 storgage vault case")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS alter_s3_vault
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

    sql """
        CREATE TABLE IF NOT EXISTS alter_s3_vault_tbl (
                C_CUSTKEY     INTEGER NOT NULL,
                C_NAME        INTEGER NOT NULL
                )
                DUPLICATE KEY(C_CUSTKEY, C_NAME)
                DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1",
                "storage_vault_name" = "alter_s3_vault"
                )
    """

    sql """
        insert into alter_s3_vault_tbl values("1", "1");
    """

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT alter_s3_vault
            PROPERTIES (
            "type"="S3",
            "s3.bucket" = "error_bucket"
            );
        """
    }, "Alter property")
    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT alter_s3_vault
            PROPERTIES (
            "type"="S3",
            "provider" = "${getS3Provider()}"
            );
        """
    }, "Alter property")

    def vault_name = "alter_s3_vault"
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
        ALTER STORAGE VAULT alter_s3_vault
        PROPERTIES (
        "type"="S3",
        "VAULT_NAME" = "alter_s3_vault",
        "s3.access_key" = "new_ak"
        );
    """

    def new_vault_name = "alter_s3_vault_new"

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
            assertTrue(vaults_info[i][2].contains(""""s3.access_key" = "new_ak"""""))
        }
    }
    assertFalse(exist)

    // failed to insert due to the wrong ak
    expectExceptionLike({
        sql """
            insert into alter_s3_vault_tbl values("2", "2");
        """
    }, "")

}
