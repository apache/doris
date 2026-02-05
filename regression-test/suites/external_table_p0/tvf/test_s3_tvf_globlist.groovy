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

suite("test_s3_tvf_globlist", "p0,external") {

    String ak = getS3AK()
    String sk = getS3SK()

    try {
        order_qt_select_1 """ SELECT * FROM S3
                            (
                                "uri" = "s3://${s3BucketName}/regression/load/data/{example_0.csv}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "s3.endpoint" = "${getS3Endpoint()}",
                                "s3.region" = "${getS3Region()}",
                                "s3.access_key" = "${ak}",
                                "s3.secret_key" = "${sk}"
                            )
                            order by c1;
                        """
    } finally {
    }

    try {
        order_qt_select_2 """ SELECT * FROM S3
                            (
                                "uri" = "s3://${s3BucketName}/regression/load/data/{example_0.csv,example_1.csv}",
                                "format" = "csv",
                                "column_separator" = ",",
                                "s3.endpoint" = "${getS3Endpoint()}",
                                "s3.region" = "${getS3Region()}",
                                "s3.access_key" = "${ak}",
                                "s3.secret_key" = "${sk}"
                            )
                            order by c1;
                        """
    } finally {
    }
}
