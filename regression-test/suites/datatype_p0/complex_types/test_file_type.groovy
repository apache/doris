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
    def structValue = """NAMED_STRUCT('uri', 'urn:file', 'offset', CAST(0 AS BIGINT),
            'size', CAST(0 AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', X'')"""
    def foldMode = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    try {
        for (def fold in [false, true]) {
            sql "SET enable_fold_constant_by_be = ${fold}"
            for (def castFunction in ["CAST", "TRY_CAST"]) {
                for (def entry : [
                        ["NULL", "FILE"],
                        ["NULL", "ARRAY<FILE>"],
                        ["NULL", "STRUCT<f:FILE>"],
                        ["NULL", "MAP<STRING,FILE>"],
                        [structValue, "FILE"],
                        ["ARRAY(${structValue})", "ARRAY<FILE>"],
                        ["NAMED_STRUCT('f', ${structValue})", "STRUCT<f:FILE>"],
                        ["MAP('f', ${structValue})", "MAP<STRING,FILE>"],
                        ["'urn:file'", "FILE"],
                        ["CAST('{}' AS JSON)", "FILE"],
                        ["CAST('{}' AS VARIANT)", "FILE"]]) {
                    test {
                        sql "SELECT ${castFunction}(${entry[0]} AS ${entry[1]})"
                        exception "can not cast"
                    }
                }
            }
        }
    } finally {
        sql "SET enable_fold_constant_by_be = ${foldMode}"
    }
}
