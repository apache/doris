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

suite("test_alter_use_path_style", "nonConcurrent") {
    def suiteName = name;
    if (!isCloudMode()) {
        logger.info("skip ${suiteName} case, because not cloud mode")
        return
    }

    if (!enableStoragevault()) {
        logger.info("skip ${suiteName} case, because storage vault not enabled")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS ${suiteName}
        PROPERTIES (
            "type"="S3",
            "s3.endpoint"="${getS3Endpoint()}",
            "s3.region" = "${getS3Region()}",
            "s3.access_key" = "${getS3AK()}",
            "s3.secret_key" = "${getS3SK()}",
            "s3.root.path" = "${suiteName}",
            "s3.bucket" = "${getS3BucketName()}",
            "s3.external_endpoint" = "",
            "provider" = "${getS3Provider()}",
            "use_path_style" = "false"
        );
    """

    sql """
        ALTER STORAGE VAULT ${suiteName}
        PROPERTIES (
            "type"="S3",
            "use_path_style" = "true"
        );
    """

    def vaultInfos = sql """ SHOW STORAGE VAULT; """
    boolean exist = false

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(suiteName)) {
            assertTrue(vaultInfos[i][2].contains("""use_path_style: true"""))
            exist = true
        }
    }
    assertTrue(exist)


    sql """
        ALTER STORAGE VAULT ${suiteName}
        PROPERTIES (
            "type"="S3",
            "use_path_style" = "false"
        );
    """

    vaultInfos = sql """ SHOW STORAGE VAULT; """
    exist = false

    for (int i = 0; i < vaultInfos.size(); i++) {
        def name = vaultInfos[i][0]
        logger.info("name is ${name}, info ${vaultInfos[i]}")
        if (name.equals(suiteName)) {
            assertTrue(vaultInfos[i][2].contains("""use_path_style: false"""))
            exist = true
        }
    }
    assertTrue(exist)

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "use_path_style" = ""
            );
        """
    }, "use_path_style cannot be empty")

    expectExceptionLike({
        sql """
            ALTER STORAGE VAULT ${suiteName}
            PROPERTIES (
            "type"="S3",
            "use_path_style" = "abc"
            );
        """
    }, "Invalid use_path_style value")
}