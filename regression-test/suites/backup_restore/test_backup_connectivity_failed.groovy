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

suite("test_backup_restore", "connectivity_failed") {
    def name = "test_name"
    def bucket = "useless_bucket"
    def prefix = "useless_prefix"
    def endpoint = getS3Endpoint()
    def region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    expectExceptionLike({
        sql """
            CREATE REPOSITORY `${name}`
            WITH S3
            ON LOCATION "s3://${bucket}/${prefix}/${name}"
            PROPERTIES
            (
                "s3.endpoint" = "http://${endpoint}",
                "s3.region" = "${region}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}"
            )
            """
    }, "Failed to create repository")
}