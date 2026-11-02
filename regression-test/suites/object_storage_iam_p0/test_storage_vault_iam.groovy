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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_storage_vault_iam") {
    if (!isCloudMode()) {
        logger.info("skip ${name} because it requires cloud mode")
        return
    }
    if (!enableStoragevault()) {
        logger.info("skip ${name} because storage vault is disabled")
        return
    }

    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def cases = config.authCases.collect { authCase ->
        return [
                name: authCase.name,
                properties: """
                    ${authCase.storageSqlProperties},
                    "s3.bucket" = "${config.bucket}",
                    "s3.external_endpoint" = "",
                    "use_path_style" = "false"
                """
        ]
    }

    cases.eachWithIndex { testCase, index ->
        def randomStr = UUID.randomUUID().toString().replace("-", "")
        def vaultName = "iam_vault_${index}_${randomStr}"
        def tableName = "test_iam_vault_${index}_${randomStr}"

        logger.info("run ${name} with ${testCase.name}")
        sql """
            CREATE STORAGE VAULT ${vaultName}
            PROPERTIES (
                "type" = "S3",
                "s3.root.path" = "${config.prefix}/test_storage_vault_iam/${testCase.name}/${randomStr}",
                ${testCase.properties}
            )
        """
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                value INT NOT NULL
            )
            DUPLICATE KEY(id, value)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "storage_vault_name" = "${vaultName}"
            )
        """
        sql "INSERT INTO ${tableName} VALUES (1, 1)"
        sql "SYNC"
        def result = sql "SELECT * FROM ${tableName}"
        assertEquals(1, result.size(), testCase.name)
        assertEquals(1, result[0][0], testCase.name)
    }
}
