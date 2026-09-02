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

suite("test_empty_first_compressed_schema", "p0,external") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName")
    String baseUri = "https://${bucket}.${s3Endpoint}/regression/tvf/test_empty_first_compressed_schema"

    order_qt_csv_empty_first_gzip """
        select id, name, metric from
        s3(
            "URI" = "${baseUri}/csv/*.csv.gz",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "FORMAT" = "csv_with_names",
            "column_separator" = ",",
            "use_path_style" = "false") order by id;
    """

    order_qt_json_empty_first_gzip """
        select id, city, metric from
        s3(
            "URI" = "${baseUri}/json/*.jsonl.gz",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "FORMAT" = "json",
            "read_json_by_line" = "true",
            "fuzzy_parse" = "true",
            "use_path_style" = "false") order by id;
    """
}
