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

suite("test_path_partition_column_collision", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    // Upload the fixture to a per-run key: missing objects yield a dummy schema, and shared keys race on cleanup.
    String key = "regression/tvf/test_path_partition_column_collision/${UUID.randomUUID()}/partition_col=from_path/data.csv"
    File fixture = new File(context.dataPath, "test_path_partition_column_collision/partition_col=from_path/data.csv")
    def client = getS3Client()
    String uri = "https://${bucket}.${s3Endpoint}/${key}"

    try {
        client.putObject(bucket, key, fixture)
        test {
            sql """
                desc function s3(
                    "URI" = "${uri}",
                    "s3.access_key" = "${ak}",
                    "s3.secret_key" = "${sk}",
                    "FORMAT" = "csv_with_names",
                    "column_separator" = ",",
                    "use_path_style" = "false", -- aliyun does not support path_style
                    "path_partition_keys" = "partition_col");
            """
            exception "Path partition column conflicts with an existing column: partition_col"
        }
    } finally {
        try {
            client.deleteObject(bucket, key)
        } finally {
            client.shutdown()
        }
    }
}
