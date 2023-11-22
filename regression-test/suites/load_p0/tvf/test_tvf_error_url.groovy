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

suite("test_tvf_error_url", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    String path = "select_tvf/no_exists_file_test"
    order_qt_select """ SELECT * FROM S3 (
                            "uri" = "http://${s3_endpoint}/${bucket}/${path}/no_exist_file1.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
                     """

    order_qt_desc """ desc function S3 (
                            "uri" = "http://${s3_endpoint}/${bucket}/${path}/no_exist_file1.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
                     """

    order_qt_select2 """ SELECT * FROM S3 (
                            "uri" = "http://${s3_endpoint}/${bucket}/${path}/*.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
                     """

    order_qt_desc2 """ desc function S3 (
                            "uri" = "http://${s3_endpoint}/${bucket}/${path}/*.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
                     """
}
