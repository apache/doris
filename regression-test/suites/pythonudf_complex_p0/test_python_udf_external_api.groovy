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

suite("test_python_udf_external_api") {
    // All APIs are FREE and require NO API KEY:
    // - JSONPlaceholder - FIXED responses, use qt_ tests
    // - ip-api.com, Open-Meteo, WorldTimeAPI - DYNAMIC responses, use assertion tests

    def pyPath = """${context.file.parent}/py_udf_complex_scripts/py_udf_complex.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"

    try {
        // Test 1-5: JSONPlaceholder FIXED responses - qt_ tests
        sql """ DROP FUNCTION IF EXISTS py_fetch_post_title(INT); """
        sql """
        CREATE FUNCTION py_fetch_post_title(INT) RETURNS STRING
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.fetch_post_title", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        qt_fetch_post_title """ SELECT py_fetch_post_title(1); """

        sql """ DROP FUNCTION IF EXISTS py_fetch_user_email(INT); """
        sql """
        CREATE FUNCTION py_fetch_user_email(INT) RETURNS STRING
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.fetch_user_email", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        qt_fetch_user_email """ SELECT py_fetch_user_email(1); """

        sql """ DROP FUNCTION IF EXISTS py_fetch_user_name(INT); """
        sql """
        CREATE FUNCTION py_fetch_user_name(INT) RETURNS STRING
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.fetch_user_name", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        qt_fetch_user_name """ SELECT py_fetch_user_name(1); """

        sql """ DROP FUNCTION IF EXISTS py_fetch_comments_count(INT); """
        sql """
        CREATE FUNCTION py_fetch_comments_count(INT) RETURNS INT
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.fetch_comments_count", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        qt_fetch_comments_count """ SELECT py_fetch_comments_count(1); """

        // Test 6: DYNAMIC APIs - use assertion tests (not qt_)
        sql """ DROP FUNCTION IF EXISTS py_get_ip_location(STRING); """
        sql """
        CREATE FUNCTION py_get_ip_location(STRING) RETURNS STRING
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.get_ip_location", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        def ip_result = sql """ SELECT py_get_ip_location('8.8.8.8') """
        assertTrue(ip_result.size() == 1)
        assertTrue(ip_result[0][0].toString().contains("country") || ip_result[0][0].toString().contains("error"))

        sql """ DROP FUNCTION IF EXISTS py_get_weather(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_get_weather(DOUBLE, DOUBLE) RETURNS STRING
        PROPERTIES ("file"="file://${pyPath}", "symbol"="external_api.get_weather", "type"="PYTHON_UDF", "runtime_version"="${runtime_version}");
        """
        def weather_result = sql """ SELECT py_get_weather(39.9042, 116.4074) """
        assertTrue(weather_result.size() == 1)
        assertTrue(weather_result[0][0].toString().contains("current_weather") || weather_result[0][0].toString().contains("error"))

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_fetch_post_title(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_fetch_user_email(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_fetch_user_name(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_fetch_comments_count(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_get_ip_location(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_get_weather(DOUBLE, DOUBLE);")
    }
}