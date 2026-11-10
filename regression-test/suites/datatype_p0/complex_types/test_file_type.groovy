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

suite("test_file_type") {
    def structType = "STRUCT<uri:STRING,offset:BIGINT,size:BIGINT," +
            "content_type:STRING,checksum:STRING,inline:VARBINARY>"
    def structValue = """NAMED_STRUCT('uri', 'urn:file', 'offset', CAST(0 AS BIGINT),
            'size', CAST(0 AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', X'')"""
    def file = "TO_FILE('file_cast_resource', NULL)"
    def foldMode = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    sql "DROP RESOURCE IF EXISTS 'file_cast_resource'"
    sql """
        CREATE RESOURCE 'file_cast_resource' PROPERTIES(
            "type"="s3", "AWS_ENDPOINT"="http://127.0.0.1:1", "AWS_REGION"="us-east-1",
            "AWS_BUCKET"="file-test", "AWS_ACCESS_KEY"="fake", "AWS_SECRET_KEY"="fake",
            "s3_validity_check"="false")
    """
    try {
        for (def fold in [false, true]) {
            sql "SET enable_fold_constant_by_be = ${fold}"
            for (def castFunction in ["CAST", "TRY_CAST"]) {
                for (def entry : [
                        ["NULL", "FILE"],
                        ["NULL", "ARRAY<FILE>"],
                        ["NULL", "STRUCT<f:FILE>"],
                        ["NULL", "MAP<STRING,FILE>"],
                        [file, "FILE"],
                        ["ARRAY(${file})", "ARRAY<FILE>"],
                        ["NAMED_STRUCT('f', ${file})", "STRUCT<f:FILE>"],
                        ["MAP('f', ${file})", "MAP<STRING,FILE>"],
                        [structValue, "FILE"],
                        ["ARRAY(${structValue})", "ARRAY<FILE>"],
                        ["NAMED_STRUCT('f', ${structValue})", "STRUCT<f:FILE>"],
                        ["MAP('f', ${structValue})", "MAP<STRING,FILE>"],
                        ["'urn:file'", "FILE"],
                        ["CAST('{}' AS JSON)", "FILE"],
                        ["CAST('{}' AS VARIANT)", "FILE"],
                        [file, structType],
                        [file, "STRING"],
                        [file, "JSON"],
                        [file, "VARIANT"],
                        ["ARRAY(${file})", "ARRAY<${structType}>"],
                        ["NAMED_STRUCT('f', ${file})", "STRUCT<f:${structType}>"],
                        ["MAP('f', ${file})", "MAP<STRING,${structType}>"]]) {
                    test {
                        sql "SELECT ${castFunction}(${entry[0]} AS ${entry[1]})"
                        exception "can not cast"
                    }
                }
            }
            quickTest("null_values_${fold}", """
                SELECT ${file}, TO_JSON(${file}),
                       ELEMENT_AT(${file}, 'uri'), FL_GET_BINARY(${file}),
                       TO_JSON(ARRAY(${file})),
                       TO_JSON(NAMED_STRUCT('f', ${file})),
                       TO_JSON(MAP('f', ${file}))
            """)
        }
    } finally {
        sql "SET enable_fold_constant_by_be = ${foldMode}"
        sql "DROP RESOURCE IF EXISTS 'file_cast_resource'"
    }
}
