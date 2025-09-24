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

suite("test_tvf_lz4_compress") {
    String ak = getS3AK()
    String sk = getS3SK()
    String region = getS3Region()
    String BucketName = context.config.otherConfigs.get("s3BucketName")

    // lz4 -> lz4frame 
    def res1 = sql """ 
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${BucketName}/oss-cn-beijing-internal.aliyuncs.com/load_p0/tvf/tvf_compress.csv.lz4",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "https://oss-cn-hongkong.aliyuncs.com",
                        "s3.region" = "${region}",
                        "column_separator" = ",",
                        "compress_type" = "lz4",
                        "format" = "csv"
                    )
                    """

    def res2 = sql """ 
                    SELECT * FROM S3
                    (
                        "uri" = "s3://${BucketName}/oss-cn-beijing-internal.aliyuncs.com/load_p0/tvf/tvf_compress.csv.lz4",
                        "s3.access_key" = "${ak}",
                        "s3.secret_key" = "${sk}",
                        "s3.endpoint" = "https://oss-cn-hongkong.aliyuncs.com",
                        "s3.region" = "${region}",
                        "column_separator" = ",",
                        "compress_type" = "lz4frame",
                        "format" = "csv"
                    )
                    """
    // check: 10 rows
    assertEquals(10, res1.size())
    assertEquals(10, res2.size())
}