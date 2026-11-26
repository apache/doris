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

suite("test_file_type_udf_disabled") {
    for (def binaryType : ["JAVA_UDF", "PYTHON_UDF"]) {
        String properties = "PROPERTIES('type'='${binaryType}', 'symbol'='evaluate'"
        if (binaryType == "PYTHON_UDF") {
            properties += ", 'runtime_version'='3.10.2'"
        }
        properties += ")"
        for (def type : ["FILE", "ARRAY<FILE>", "MAP<STRING,FILE>", "STRUCT<f:FILE>",
                         "ARRAY<STRUCT<f:FILE>>"]) {
            for (def kind : ["FUNCTION", "AGGREGATE FUNCTION", "TABLES FUNCTION"]) {
                String returnType = kind == "TABLES FUNCTION" ? "ARRAY<INT>" : "INT"
                test {
                    sql """CREATE ${kind} test_file_udf_argument(${type})
                           RETURNS ${returnType} ${properties}"""
                    exception "${binaryType} does not support argument 1"
                }
                String outputType = kind == "TABLES FUNCTION" ? "ARRAY<${type}>" : type
                test {
                    sql """CREATE ${kind} test_file_udf_return(INT)
                           RETURNS ${outputType} ${properties}"""
                    exception "${binaryType} does not support return"
                }
            }
            test {
                sql """CREATE AGGREGATE FUNCTION test_file_udf_intermediate(INT)
                       RETURNS INT INTERMEDIATE ${type} ${properties}"""
                exception "${binaryType} does not support intermediate"
            }
        }
    }
}
