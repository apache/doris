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

suite("test_tvf_empty_file", "p0") {
       String ak = getS3AK()
       String sk = getS3SK()
       String s3_endpoint = getS3Endpoint()
       String region = getS3Region()
       String bucket = context.config.otherConfigs.get("s3BucketName");

       String path = "regression/datalake"

       // ${path}/empty_file_test.csv is an empty file
       // so it should return empty sets.
       order_qt_select """ SELECT * FROM S3 (
                                   "uri" = "http://${bucket}.${s3_endpoint}/${path}/empty_file_test.csv",
                                   "ACCESS_KEY"= "${ak}",
                                   "SECRET_KEY" = "${sk}",
                                   "format" = "csv",
                                   "region" = "${region}"
                                   );
                            """

       order_qt_desc """ desc function S3 (
                                   "uri" = "http://${bucket}.${s3_endpoint}/${path}/empty_file_test.csv",
                                   "ACCESS_KEY"= "${ak}",
                                   "SECRET_KEY" = "${sk}",
                                   "format" = "csv",
                                   "region" = "${region}"
                                   );
                            """

       // ${path}/empty_file_test*.csv matches 3 files:
       // empty_file_test.csv, empty_file_test_1.csv, empty_file_test_2.csv
       // empty_file_test.csv is an empty file, but
       // empty_file_test_1.csv and empty_file_test_2.csv have data
       // so it should return data of empty_file_test_1.csv and empty_file_test_2.cs
       order_qt_select2 """ SELECT * FROM S3 (
                                   "uri" = "http://${bucket}.${s3_endpoint}/${path}/empty_file_test*.csv",
                                   "ACCESS_KEY"= "${ak}",
                                   "SECRET_KEY" = "${sk}",
                                   "format" = "csv",
                                   "region" = "${region}"
                                   ) order by c1;
                            """

       order_qt_des2 """ desc function S3 (
                                   "uri" = "http://${bucket}.${s3_endpoint}/${path}/empty_file_test*.csv",
                                   "ACCESS_KEY"= "${ak}",
                                   "SECRET_KEY" = "${sk}",
                                   "format" = "csv",
                                   "region" = "${region}"
                                   );
                            """
}
