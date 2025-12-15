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

suite("test_mysql_compat_var_whitelist") {

    // Test 1: Verify unknown variables throw errors (default behavior)
    // This ensures the whitelist doesn't break normal error handling
    test {
        sql "set unknown_variable_12345 = 1"
        exception "Unknown system variable"
    }

    test {
        sql "set @@nonexistent_var = 0"
        exception "Unknown system variable"
    }

    // Test 2: Verify normal Doris variables still work correctly
    def originalTimeout = sql "show variables where variable_name = 'query_timeout'"
    sql "set query_timeout = 999"
    def modifiedTimeout = sql "show variables where variable_name = 'query_timeout'"
    assertTrue(modifiedTimeout[0][1] == "999")
    sql "set query_timeout = ${originalTimeout[0][1]}"

    // Test 3: If whitelist is configured, test whitelisted variables
    // To enable these tests, configure fe.conf with:
    //   mysql_compat_var_whitelist = foreign_key_checks,unique_checks,sql_notes
    // Then set environment variable: export TEST_MYSQL_COMPAT_WHITELIST=true

    if (System.getenv("TEST_MYSQL_COMPAT_WHITELIST") == "true") {
        logger.info("Testing with configured whitelist...")

        sql "set foreign_key_checks = 0"
        sql "set foreign_key_checks = 1"
        sql "set unique_checks = 0"
        sql "set sql_notes = 1"

        sql "set @@foreign_key_checks = 0"
        sql "set @@unique_checks = 1"

        sql "set session foreign_key_checks = 0"
        sql "set @@session.unique_checks = 1"

        sql "set FOREIGN_KEY_CHECKS = 0"
        sql "set Foreign_Key_Checks = 1"
        sql "set UNIQUE_CHECKS = 0"

        sql "set foreign_key_checks = 0, unique_checks = 0"
        sql "set @@session.foreign_key_checks = 1, @@session.unique_checks = 1"

        sql "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */"
        sql "/*!40014 SET FOREIGN_KEY_CHECKS=1 */"

        sql "set foreign_key_checks = ON"
        sql "set foreign_key_checks = OFF"
        sql "set foreign_key_checks = TRUE"
        sql "set foreign_key_checks = FALSE"

        test {
            sql "set still_unknown_variable = 1"
            exception "Unknown system variable"
        }

        logger.info("Whitelist tests passed")
    } else {
        logger.info("Skipping whitelist configuration tests.")
    }
}
