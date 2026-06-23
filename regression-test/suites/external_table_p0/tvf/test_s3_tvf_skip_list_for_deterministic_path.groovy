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

suite("test_s3_tvf_skip_list_for_deterministic_path", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    order_qt_select """ select * from S3 (
                "uri" = "s3://${bucket}/dir_deny_list_objects/{test_s3_tvf_skip_list_for_exact_path.csv,missing.csv}",
                "format" = "csv",
                "column_separator" = ",",
                "csv_schema" = "k1:int;k2:string",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${region}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "use_path_style" = "false"
            ) order by k1, k2; """
}
