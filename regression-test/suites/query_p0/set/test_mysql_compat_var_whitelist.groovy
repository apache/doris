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

suite("test_mysql_compat_var_whitelist", "nonConcurrent") {

    // Test 1: Verify unknown variables throw errors when whitelist is empty (default)
    test {
        sql "set unknown_variable_12345 = 1"
        exception "Unknown system variable"
    }

    test {
        sql "set @@nonexistent_var = 0"
        exception "Unknown system variable"
    }

    test {
        sql "set foreign_key_checks = 0"
        exception "Unknown system variable"
    }

    // Test 2: Configure whitelist using ADMIN SET FRONTEND CONFIG
    sql """ADMIN SET FRONTEND CONFIG ("mysql_compat_var_whitelist" = "foreign_key_checks,unique_checks,sql_notes")"""

    // Test 3: Basic SET syntax - whitelisted variables should not throw errors
    sql "set foreign_key_checks = 0"
    sql "set foreign_key_checks = 1"
    sql "set unique_checks = 0"
    sql "set unique_checks = 1"
    sql "set sql_notes = 0"
    sql "set sql_notes = 1"

    // Test 4: @@ syntax
    sql "set @@foreign_key_checks = 0"
    sql "set @@unique_checks = 1"
    sql "set @@sql_notes = 0"

    // Test 5: session syntax
    sql "set session foreign_key_checks = 0"
    sql "set @@session.unique_checks = 1"
    sql "set @@session.sql_notes = 0"

    // Test 6: Case insensitivity
    sql "set FOREIGN_KEY_CHECKS = 0"
    sql "set Foreign_Key_Checks = 1"
    sql "set UNIQUE_CHECKS = 0"
    sql "set Unique_Checks = 1"

    // Test 7: Combined SET statements (common in mysqldump)
    sql "set foreign_key_checks = 0, unique_checks = 0"
    sql "set @@session.foreign_key_checks = 1, @@session.unique_checks = 1"
    sql "set foreign_key_checks = 0, unique_checks = 0, sql_notes = 0"

    // Test 8: MySQL dump style with version comments
    sql "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */"
    sql "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */"
    sql "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */"
    sql "/*!40014 SET FOREIGN_KEY_CHECKS=1 */"
    sql "/*!40014 SET UNIQUE_CHECKS=1 */"
    sql "/*!40111 SET SQL_NOTES=1 */"

    // Test 9: Various boolean value formats
    sql "set foreign_key_checks = ON"
    sql "set foreign_key_checks = OFF"
    sql "set foreign_key_checks = TRUE"
    sql "set foreign_key_checks = FALSE"
    sql "set unique_checks = ON"
    sql "set unique_checks = OFF"

    // Test 10: Verify non-whitelisted variables still throw errors
    test {
        sql "set still_unknown_variable = 1"
        exception "Unknown system variable"
    }

    test {
        sql "set another_nonexistent_var = true"
        exception "Unknown system variable"
    }

    // Test 11: Verify normal Doris variables still work
    def originalTimeout = sql "show variables where variable_name = 'query_timeout'"
    sql "set query_timeout = 999"
    def modifiedTimeout = sql "show variables where variable_name = 'query_timeout'"
    assertTrue(modifiedTimeout[0][1] == "999")
    sql "set query_timeout = ${originalTimeout[0][1]}"

    // Test 12: Clear whitelist and verify variables throw errors again
    sql """ADMIN SET FRONTEND CONFIG ("mysql_compat_var_whitelist" = "")"""

    test {
        sql "set foreign_key_checks = 0"
        exception "Unknown system variable"
    }

    test {
        sql "set unique_checks = 0"
        exception "Unknown system variable"
    }
}
