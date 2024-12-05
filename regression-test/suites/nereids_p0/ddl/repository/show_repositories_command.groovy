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

suite("show_repositories_command") {
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String repoName = "test_show_repositories"

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql "DROP REPOSITORY `${repoName}`"

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

    def show_repo = checkNereidsExecuteWithResult("""SHOW REPOSITORIES;""").toString();
    assertTrue(show_repo.contains("${repoName}"))

    sql """DROP REPOSITORY `${repoName}`;"""

    def show_repo_after_drop = checkNereidsExecuteWithResult("""SHOW REPOSITORIES;""").toString();
    assertFalse(show_repo_after_drop.contains("${repoName}"))
}
