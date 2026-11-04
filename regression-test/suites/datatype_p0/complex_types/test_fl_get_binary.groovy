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

suite("test_fl_get_binary") {
    qt_nulls "SELECT FL_GET_BINARY(NULL), FL_GET_BINARY(CAST(NULL AS FILE))"
    for (def field in ["inline", "INLINE", "InLiNe"]) {
        test {
            sql "SELECT ELEMENT_AT(CAST(NULL AS FILE), '${field}')"
            exception "use FL_GET_BINARY"
        }
        test {
            sql "SELECT (CAST(NULL AS FILE))['${field}']"
            exception "use FL_GET_BINARY"
        }
    }
    for (def argument in ["1", "'uri'", "CAST('{}' AS JSON)", "CAST('{}' AS VARIANT)",
                          "NAMED_STRUCT('uri', 'urn:x')", "ARRAY(CAST(NULL AS FILE))"]) {
        test {
            sql "SELECT FL_GET_BINARY(${argument})"
            exception "Can not find the compatibility function signature"
        }
    }
    test {
        sql "SELECT FL_GET_BINARY()"
        exception "Can not found function"
    }
    test {
        sql "SELECT FL_GET_BINARY(CAST(NULL AS FILE), 'uri')"
        exception "Can not found function"
    }
}
