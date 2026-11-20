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
    def foldMode = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    try {
        for (def fold in [false, true]) {
            sql "SET enable_fold_constant_by_be = ${fold}"
            for (def castFunction in ["CAST", "TRY_CAST"]) {
                for (def entry : [
                        [structValue, "FILE"],
                        ["ARRAY(${structValue})", "ARRAY<FILE>"],
                        ["NAMED_STRUCT('f', ${structValue})", "STRUCT<f:FILE>"],
                        ["MAP('f', ${structValue})", "MAP<STRING,FILE>"],
                        ["'urn:file'", "FILE"],
                        ["CAST('{}' AS JSON)", "FILE"],
                        ["CAST('{}' AS VARIANT)", "FILE"],
                        ["CAST(NULL AS FILE)", structType],
                        ["CAST(NULL AS FILE)", "STRING"],
                        ["CAST(NULL AS FILE)", "JSON"],
                        ["CAST(NULL AS FILE)", "VARIANT"],
                        ["ARRAY(CAST(NULL AS FILE))", "ARRAY<${structType}>"],
                        ["NAMED_STRUCT('f', CAST(NULL AS FILE))", "STRUCT<f:${structType}>"],
                        ["MAP('f', CAST(NULL AS FILE))", "MAP<STRING,${structType}>"]]) {
                    test {
                        sql "SELECT ${castFunction}(${entry[0]} AS ${entry[1]})"
                        exception "can not cast"
                    }
                }
            }
            quickTest("null_values_${fold}", """
                SELECT CAST(NULL AS FILE), TO_JSON(CAST(NULL AS FILE)),
                       ELEMENT_AT(CAST(NULL AS FILE), 'uri'), FL_GET_BINARY(CAST(NULL AS FILE)),
                       TO_JSON(ARRAY(CAST(NULL AS FILE))),
                       TO_JSON(NAMED_STRUCT('f', CAST(NULL AS FILE))),
                       TO_JSON(MAP('f', CAST(NULL AS FILE)))
            """)
        }
    } finally {
        sql "SET enable_fold_constant_by_be = ${foldMode}"
    }
}
