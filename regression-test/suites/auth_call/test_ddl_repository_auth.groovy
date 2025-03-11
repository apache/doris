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

suite("test_ddl_repository_auth","p0,auth_call") {
    String user = 'test_ddl_repository_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_repository_auth_db'
    String repositoryName = 'test_ddl_repository_auth_rps'

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    //cloud-mode
    if (isCloudMode()) {
        return
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""DROP REPOSITORY `${repositoryName}`;""")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE REPOSITORY `${repositoryName}`
                    WITH S3
                    ON LOCATION "s3://${bucket}/${repositoryName}"
                    PROPERTIES
                    (
                        "s3.endpoint" = "http://${endpoint}",
                        "s3.region" = "${region}",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}"
                    )"""
            exception "denied"
        }
        test {
            sql """SHOW CREATE REPOSITORY for ${repositoryName};"""
            exception "denied"
        }

        test {
            sql """DROP REPOSITORY `${repositoryName}`;"""
            exception "denied"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE REPOSITORY `${repositoryName}`
                WITH S3
                ON LOCATION "s3://${bucket}/${repositoryName}"
                PROPERTIES
                (
                    "s3.endpoint" = "http://${endpoint}",
                    "s3.region" = "${region}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}"
                )"""
        def res = sql """SHOW CREATE REPOSITORY for ${repositoryName};"""
        assertTrue(res.size() > 0)

        sql """DROP REPOSITORY `${repositoryName}`;"""
        test {
            sql """SHOW CREATE REPOSITORY for ${repositoryName};"""
            exception "repository not exist"
        }
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
