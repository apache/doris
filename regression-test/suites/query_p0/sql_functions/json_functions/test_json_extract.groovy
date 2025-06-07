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

suite("test_json_extract") {
    qt_sql_string1 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', '\$.k1'); """
    qt_sql_string2 """ SELECT JSON_EXTRACT_STRING(null, '\$.k1'); """
    qt_sql_string3 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', NULL); """
    qt_sql_string4 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":{"sub_key": 1234.56}}', '\$.k2.sub_key'); """
    qt_sql_string5 """ SELECT JSON_EXTRACT_STRING(json_array("abc", 123, STR_TO_DATE('2025-06-05 14:47:01', '%Y-%m-%d %H:%i:%s')), '\$.[2]'); """
    qt_sql_string6 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2": null}', '\$.k2'); """
    qt_sql_string7 """ SELECT JSON_EXTRACT_STRING('{"k1":"v31","k2":300}', '\$.k3'); """

    test {
        sql """ SELECT JSON_EXTRACT_STRING('{"id": 123, "name": "doris"}', '\$.'); """
        exception "Invalid Json Path for value: \$."
    }
}
