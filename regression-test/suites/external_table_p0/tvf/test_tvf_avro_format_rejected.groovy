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


// Doris no longer reads standalone Avro files: #69601 removed the BE reader, and FE rejects
// format=avro while it analyzes a table function's properties, before it validates the storage
// properties or lists a file. These calls therefore fail with that message although the bucket, the
// credentials and the HDFS address are made up. Before #69601 FE accepted the format and the query
// failed later in BE with "Not supported file format in fetch table schema: 12".
suite("test_tvf_avro_format_rejected", "p0,external") {
    for (String format : ["avro", "AVRO"]) {
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "s3://doris-regression-missing-bucket/avro/data.avro",
                    "s3.access_key" = "ak",
                    "s3.secret_key" = "sk",
                    "s3.region" = "us-east-1",
                    "format" = "${format}")
            """
            exception "Avro file reads are no longer supported"
        }
        test {
            sql """
                SELECT * FROM hdfs(
                    "uri" = "hdfs://127.0.0.1:8020/doris-regression/avro/data.avro",
                    "fs.defaultFS" = "hdfs://127.0.0.1:8020",
                    "format" = "${format}")
            """
            exception "Avro file reads are no longer supported"
        }
    }
}
