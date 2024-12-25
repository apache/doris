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

suite("test_s3_vault_path_start_with_slash", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip ${name} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${name} case")
        return
    }

    def tableName = "table_test_s3_vault_path_start_with_slash"
    try {
        def vault_name = "test_s3_vault_path_start_with_slash_vault"
        sql """
            CREATE STORAGE VAULT IF NOT EXISTS ${vault_name}
            PROPERTIES (
                "type"="S3",
                "s3.endpoint"="${getS3Endpoint()}",
                "s3.region" = "${getS3Region()}",
                "s3.access_key" = "${getS3AK()}",
                "s3.secret_key" = "${getS3SK()}",
                "s3.root.path" = "/test_s3_vault_path_start_with_slash_vault",
                "s3.bucket" = "${getS3BucketName()}",
                "s3.external_endpoint" = "",
                "provider" = "${getS3Provider()}",
                "set_as_default" = "true"
            );
        """

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
                CREATE TABLE ${tableName} (
                    `key` INT,
                    value INT
                ) DUPLICATE KEY (`key`) 
                DISTRIBUTED BY HASH (`key`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "storage_vault_name" = "${vault_name}"
                )
            """

        sql """ insert into ${tableName} values(1, 1); """
        sql """ sync;"""
        def result = sql """ select * from ${tableName}; """
        logger.info("result:${result}");
        assertTrue(result.size() == 1)
        assertTrue(result[0][0].toInteger() == 1)
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
