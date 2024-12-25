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

import org.junit.Assert;

suite("show_create_repository_command") {
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String repoName = "test_show_create_repository"
    String readOnlyRepoName = "test_show_create_read_only_repository"

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql "DROP REPOSITORY `${repoName}`"
    try_sql "DROP REPOSITORY `${readOnlyRepoName}`"

    // create S3 repo
    sql """CREATE REPOSITORY `${repoName}`
            WITH S3
            ON LOCATION "s3://${bucket}/${repoName}"
            PROPERTIES
            (
                "s3.endpoint" = "http://${endpoint}",
                "s3.region" = "${region}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}"
            )"""

    // create S3 read only repo
    sql """CREATE READ ONLY REPOSITORY `${readOnlyRepoName}`
            WITH S3
            ON LOCATION "s3://${bucket}/${readOnlyRepoName}"
            PROPERTIES
            (
                "s3.endpoint" = "http://${endpoint}",
                "s3.region" = "${region}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}"
            )"""

    def create_repo = checkNereidsExecuteWithResult("""SHOW CREATE REPOSITORY for `${repoName}`;""").toString();
    assertTrue(create_repo.contains("${repoName}"))
    assertTrue(create_repo.contains("s3://${bucket}/${repoName}"))

    def create_read_only_repo = checkNereidsExecuteWithResult("""SHOW CREATE REPOSITORY for `${readOnlyRepoName}`;""").toString();
    assertTrue(create_read_only_repo.contains("${readOnlyRepoName}"))
    assertTrue(create_read_only_repo.contains("READ ONLY"))

    sql """DROP REPOSITORY `${repoName}`;"""
    sql """DROP REPOSITORY `${readOnlyRepoName}`;"""

    try {
        sql """SHOW CREATE REPOSITORY for `${repoName}`;"""
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("repository not exist."))
    }
}
