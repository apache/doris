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

suite("test_json_uppercase_keys", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName")
    // Upload the fixture to a per-run key: missing objects yield a dummy schema, and shared keys race on cleanup.
    String key = "regression/tvf/test_json_uppercase_keys/${UUID.randomUUID()}/case_keys.jsonl"
    File fixture = new File(context.dataPath, "test_json_uppercase_keys/case_keys.jsonl")
    def client = getS3Client()
    String uri = "https://${bucket}.${s3Endpoint}/${key}"

    try {
        client.putObject(bucket, key, fixture)
        order_qt_json_uppercase_keys """
            select id, city, metric from
            s3(
                "URI" = "${uri}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "FORMAT" = "json",
                "read_json_by_line" = "true",
                "fuzzy_parse" = "true",
                "use_path_style" = "false") order by id;
        """

        order_qt_json_uppercase_keys_with_jsonpaths """
            select id, city, metric from
            s3(
                "URI" = "${uri}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "FORMAT" = "json",
                "read_json_by_line" = "true",
                "fuzzy_parse" = "true",
                "jsonpaths" = '["\$.ID", "\$.City", "\$.Metric"]',
                "use_path_style" = "false") order by id;
        """
    } finally {
        try {
            client.deleteObject(bucket, key)
        } finally {
            client.shutdown()
        }
    }
}
