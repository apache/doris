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

suite("test_create_and_drop_repository", "backup_restore") {
    String repoName = "test_create_and_drop_repository"

    try_sql "DROP REPOSITORY `${repoName}`"

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def filterShowRepoResult = { result, name ->
        for (record in result) {
            if (record[1] == name)
                return record
        }
        null
    }

    // case 1. S3 repo
    sql """
    CREATE REPOSITORY `${repoName}`
    WITH S3
    ON LOCATION "s3://${bucket}/${repoName}"
    PROPERTIES
    (
        "s3.endpoint" = "http://${endpoint}",
        "s3.region" = "${region}",
        "s3.access_key" = "${ak}",
        "s3.secret_key" = "${sk}"
    )
        """

    def result = sql """ SHOW REPOSITORIES """
    def repo = filterShowRepoResult(result, repoName)
    assertTrue(repo != null)

    sql "DROP REPOSITORY `${repoName}`"

    result = sql """ SHOW REPOSITORIES """
    repo = filterShowRepoResult(result, repoName)
    assertTrue(repo == null)

    // case 2. S3 read only repo
    sql """
    CREATE READ ONLY REPOSITORY `${repoName}`
    WITH S3
    ON LOCATION "s3://${bucket}/${repoName}"
    PROPERTIES
    (
        "s3.endpoint" = "http://${endpoint}",
        "s3.region" = "${region}",
        "s3.access_key" = "${ak}",
        "s3.secret_key" = "${sk}"
    )
        """

    result = sql """ SHOW REPOSITORIES """
    repo = filterShowRepoResult(result, repoName)
    assertTrue(repo != null)

    sql "DROP REPOSITORY `${repoName}`"

    result = sql """ SHOW REPOSITORIES """
    repo = filterShowRepoResult(result, repoName)
    assertTrue(repo == null)

    if (enableHdfs()) {
        // case 3. hdfs repo
        String hdfsFs = getHdfsFs()
        String hdfsUser = getHdfsUser()
        String dataDir = getHdfsDataDir()

        sql """
        CREATE REPOSITORY `${repoName}`
        WITH hdfs
        ON LOCATION "${dataDir}${repoName}"
        PROPERTIES
        (
            "fs.defaultFS" = "${hdfsFs}",
            "hadoop.username" = "${hdfsUser}"
        )
        """

        result = sql """ SHOW REPOSITORIES """
        repo = filterShowRepoResult(result, repoName)
        assertTrue(repo != null)

        sql "DROP REPOSITORY `${repoName}`"

        result = sql """ SHOW REPOSITORIES """
        repo = filterShowRepoResult(result, repoName)
        assertTrue(repo == null)

        // case 4. hdfs read only repo
        sql """
        CREATE READ ONLY REPOSITORY `${repoName}`
        WITH hdfs
        ON LOCATION "${dataDir}${repoName}"
        PROPERTIES
        (
            "fs.defaultFS" = "${hdfsFs}",
            "hadoop.username" = "${hdfsUser}"
        )
        """

        result = sql """ SHOW REPOSITORIES """
        repo = filterShowRepoResult(result, repoName)
        assertTrue(repo != null)

        sql "DROP REPOSITORY `${repoName}`"

        result = sql """ SHOW REPOSITORIES """
        repo = filterShowRepoResult(result, repoName)
        assertTrue(repo == null)
    }
}
