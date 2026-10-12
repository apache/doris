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

suite("test_user_authentication_binding", "p0,auth") {
    sql "DROP USER IF EXISTS 'binding_regression'@'%'"
    sql "DROP USER IF EXISTS 'binding_regression'@'10.%'"
    sql "DROP AUTHENTICATION INTEGRATION IF EXISTS binding_regression_primary"
    sql "DROP AUTHENTICATION INTEGRATION IF EXISTS binding_regression_backup"

    sql """CREATE AUTHENTICATION INTEGRATION binding_regression_primary
        PROPERTIES ('type'='password')"""
    sql """CREATE AUTHENTICATION INTEGRATION binding_regression_backup
        PROPERTIES ('type'='password')"""

    test {
        sql """CREATE USER 'binding_regression' IDENTIFIED WITH
            AUTHENTICATION INTEGRATION binding_regression_missing"""
        exception "does not exist"
    }
    test {
        sql """ALTER USER 'root' IDENTIFIED WITH AUTHENTICATION INTEGRATION binding_regression_primary"""
        exception "Can not bind root or admin"
    }
    test {
        sql """ALTER USER 'admin' IDENTIFIED WITH AUTHENTICATION INTEGRATION binding_regression_primary"""
        exception "Can not bind root or admin"
    }

    try {
        sql """CREATE USER 'binding_regression' IDENTIFIED WITH AUTHENTICATION INTEGRATION
            binding_regression_primary AS 'external:etl' COMMENT 'bound regression account'"""
    } catch (Exception e) {
        if (!e.getMessage().contains("No authentication plugin factory found for type: password")) {
            throw e
        }
        logger.info("User binding regression requires the optional password authentication plugin")
        return
    }
    order_qt_created "SHOW CREATE USER 'binding_regression'"
    for (String option : ['ACCOUNT_LOCK', "REQUIRE SAN 'DNS:binding.example'", "COMMENT 'changed'"]) {
        test {
            sql """ALTER USER 'binding_regression' IDENTIFIED WITH AUTHENTICATION INTEGRATION
                binding_regression_backup ${option}"""
            exception "Only support doing one type of operation at one time"
        }
    }
    test {
        sql "DROP AUTHENTICATION INTEGRATION binding_regression_primary"
        exception "still has bound users"
    }
    test {
        sql "SET PASSWORD FOR 'binding_regression' = PASSWORD('local')"
        exception "use ALTER USER ... IDENTIFIED BY"
    }
    for (String option : ['PASSWORD_EXPIRE NEVER', 'PASSWORD_HISTORY 0',
                          'FAILED_LOGIN_ATTEMPTS 0', 'PASSWORD_LOCK_TIME 0 SECOND']) {
        test {
            sql "ALTER USER 'binding_regression' ${option}"
            exception "Password policies are not supported"
        }
    }
    sql "ALTER USER 'binding_regression' ACCOUNT_LOCK"
    order_qt_locked "SHOW CREATE USER 'binding_regression'"
    sql "ALTER USER 'binding_regression' ACCOUNT_UNLOCK"
    sql "ALTER USER 'binding_regression' COMMENT 'same binding'"
    order_qt_preserved "SHOW CREATE USER 'binding_regression'"
    sql """ALTER USER 'binding_regression' IDENTIFIED WITH AUTHENTICATION INTEGRATION
        binding_regression_backup AS 'external:backup'"""
    order_qt_rebound "SHOW CREATE USER 'binding_regression'"
    def previousSqlMode = sql("SELECT @@sql_mode")[0][0]
    try {
        sql "SET sql_mode='NO_BACKSLASH_ESCAPES'"
        sql """ALTER USER 'binding_regression' IDENTIFIED WITH AUTHENTICATION INTEGRATION
            binding_regression_backup AS 'external\\identity'"""
        order_qt_escaped_identity "SHOW CREATE USER 'binding_regression'"
    } finally {
        sql "SET sql_mode='${previousSqlMode}'"
    }
    sql """ALTER USER 'binding_regression' IDENTIFIED WITH AUTHENTICATION INTEGRATION
        binding_regression_backup"""
    order_qt_cleared_identity "SHOW CREATE USER 'binding_regression'"
    sql "CREATE USER 'binding_regression'@'10.%' IDENTIFIED BY 'local'"
    test {
        sql "ALTER USER 'binding_regression'@'10.%' IDENTIFIED BY 'changed' ACCOUNT_LOCK"
        exception "Only support doing one type of operation at one time"
    }
    sql "ALTER USER 'binding_regression' IDENTIFIED BY 'local'"
    order_qt_unbound "SHOW CREATE USER 'binding_regression'"
    sql "DROP AUTHENTICATION INTEGRATION binding_regression_primary"
    sql "DROP AUTHENTICATION INTEGRATION binding_regression_backup"
}
