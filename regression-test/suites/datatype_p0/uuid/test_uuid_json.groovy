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

// Checklist: A06 E02 G03 G05 G14 H01 H03.
suite("test_uuid_json", "p0") {
    for (boolean strict : [false, true]) {
        sql "SET enable_strict_cast = ${strict}"
        qt_json_uuid_array """
            SELECT CAST(CAST('["00112233-4455-6677-8899-aabbccddeeff", null,
                              "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"]' AS JSON) AS ARRAY<UUID>)
        """
        qt_json_uuid_struct """
            SELECT CAST(CAST('{"u":"00112233-4455-6677-8899-aabbccddeeff"}' AS JSON)
                        AS STRUCT<u:UUID>)
        """
        qt_json_uuid_empty "SELECT CAST(CAST('[]' AS JSON) AS ARRAY<UUID>)"
        if (strict) {
            test {
                sql "SELECT CAST(CAST('[\"invalid\"]' AS JSON) AS ARRAY<UUID>)"
                exception "UUID"
            }
            test {
                sql "SELECT CAST(CAST('{\"u\":\"invalid\"}' AS JSON) AS STRUCT<u:UUID>)"
                exception "UUID"
            }
        } else {
            qt_json_uuid_invalid_array "SELECT CAST(CAST('[\"invalid\", 123, null]' AS JSON) AS ARRAY<UUID>)"
            qt_json_uuid_invalid_struct "SELECT CAST(CAST('{\"u\":\"invalid\"}' AS JSON) AS STRUCT<u:UUID>)"
        }
    }
    sql "SET enable_strict_cast = false"
}
