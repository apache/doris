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

import java.util.stream.Collectors;
import java.util.stream.Stream;

suite("test_create_vault_concurrently", "nonConcurrent") {
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
    def s3VaultName = "s3_" + randomStr

    def future1 = thread("threadName1") {
        for (int i = 0; i < 100; i++) {
            try_sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
                PROPERTIES (
                    "type"="S3",
                    "s3.endpoint"="${getS3Endpoint()}",
                    "s3.region" = "${getS3Region()}",
                    "s3.access_key" = "${getS3AK()}",
                    "s3.secret_key" = "${getS3SK()}",
                    "s3.root.path" = "${s3VaultName}",
                    "s3.bucket" = "${getS3BucketName()}",
                    "s3.external_endpoint" = "",
                    "provider" = "${getS3Provider()}",
                    "use_path_style" = "false"
                );
            """
        }
    }

    def future2 = thread("threadName2") {
        for (int i = 0; i < 100; i++) {
            try_sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
                PROPERTIES (
                    "type"="S3",
                    "s3.endpoint"="${getS3Endpoint()}",
                    "s3.region" = "${getS3Region()}",
                    "s3.access_key" = "${getS3AK()}",
                    "s3.secret_key" = "${getS3SK()}",
                    "s3.root.path" = "${s3VaultName}",
                    "s3.bucket" = "${getS3BucketName()}",
                    "s3.external_endpoint" = "",
                    "provider" = "${getS3Provider()}",
                    "use_path_style" = "false"
                );
            """
        }
    }

    def future3 = thread("threadName3") {
        for (int i = 0; i < 100; i++) {
            try_sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
                PROPERTIES (
                    "type"="S3",
                    "s3.endpoint"="${getS3Endpoint()}",
                    "s3.region" = "${getS3Region()}",
                    "s3.access_key" = "${getS3AK()}",
                    "s3.secret_key" = "${getS3SK()}",
                    "s3.root.path" = "${s3VaultName}",
                    "s3.bucket" = "${getS3BucketName()}",
                    "s3.external_endpoint" = "",
                    "provider" = "${getS3Provider()}",
                    "use_path_style" = "false"
                );
            """
        }
    }

    def future4 = thread("threadName4") {
        for (int i = 0; i < 100; i++) {
            try_sql """
                CREATE STORAGE VAULT IF NOT EXISTS ${s3VaultName}
                PROPERTIES (
                    "type"="S3",
                    "s3.endpoint"="${getS3Endpoint()}",
                    "s3.region" = "${getS3Region()}",
                    "s3.access_key" = "${getS3AK()}",
                    "s3.secret_key" = "${getS3SK()}",
                    "s3.root.path" = "${s3VaultName}",
                    "s3.bucket" = "${getS3BucketName()}",
                    "s3.external_endpoint" = "",
                    "provider" = "${getS3Provider()}",
                    "use_path_style" = "false"
                );
            """
        }
    }

    // equals to combineFutures([future1, future2, future3, future4]), which [] is a Iterable<ListenableFuture>
    def combineFuture = combineFutures(future1, future2, future3, future4)
    // or you can use lazyCheckThread action(see lazyCheck_action.groovy), and not have to check exception from futures.
    List<List<List<Object>>> result = combineFuture.get()
    logger.info("${result}")

    boolean s3VaultExisted = false;
    def vaults_info = try_sql """ SHOW STORAGE VAULTS """

    for (int i = 0; i < vaults_info.size(); i++) {
        def name = vaults_info[i][0]
        if (name.equals(s3VaultName)) {
            s3VaultExisted = true;
        }
    }
    assertTrue(s3VaultExisted)
}
