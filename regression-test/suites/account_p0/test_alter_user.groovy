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

suite("test_alter_user", "account,nonConcurrent") {

    sql """drop user if exists test_auth_user2"""
    sql """drop user if exists test_auth_user3"""
    sql """drop user if exists test_auth_user4"""
    
    // 2. test password history
    sql """set global password_history=0""" // disabled
    sql """create user test_auth_user2 identified by '12345' password_history default"""
    sql """grant all on *.* to test_auth_user2"""
    sql """alter user test_auth_user2 identified by '12345'"""
    sql """set password for 'test_auth_user2' = password('12345')"""
    
    sql """set global password_history=1""" // set to 1
    test {
        sql """alter user test_auth_user2 identified by '12345'"""
        exception "Cannot use these credentials for 'test_auth_user2'@'%' because they contradict the password history policy"
    }

    sql """alter user test_auth_user2 password_history 0"""
    sql """set password for 'test_auth_user2' = password('12345')"""
    
    def result1 = connect(user = 'test_auth_user2', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    sql """alter user test_auth_user2 password_history 2"""
    sql """alter user test_auth_user2 identified by 'abc12345'"""
    sql """alter user test_auth_user2 identified by 'abc123456'"""
    test {
        sql """alter user test_auth_user2 identified by 'abc12345'"""
        exception "Cannot use these credentials for 'test_auth_user2'@'%' because they contradict the password history policy"
    }
    result1 = connect(user = 'test_auth_user2', password = 'abc123456', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sql """set global password_history=0""" // set to disabled

    // 3. test FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME
    sql """create user test_auth_user3 identified by '12345' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 DAY"""
    sql """grant all on *.* to test_auth_user3"""
         
    // login success in multi times
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    // login failed in 2 times
    try {
        connect(user = 'test_auth_user3', password = 'wrong', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3"), e.getMessage())
    } 
    try {
        connect(user = 'test_auth_user3', password = 'wrong', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3"), e.getMessage())
    } 
    // login with correct password but also failed
    try {
        connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3'@'%'. Account is blocked for 86400 second(s) (86400 second(s) remaining) due to 2 consecutive failed logins."), e.getMessage())
    } 

    // unlock user and login again
    sql """alter user test_auth_user3 account_unlock"""
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    // alter PASSWORD_LOCK_TIME
    sql """alter user test_auth_user3 PASSWORD_LOCK_TIME 5 SECOND"""
    // login failed in 2 times to lock the accout again
    try {
        connect(user = 'test_auth_user3', password = 'wrong', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3"), e.getMessage())
    } 
    try {
        connect(user = 'test_auth_user3', password = 'wrong', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3"), e.getMessage())
    } 
    // login with correct password but also failed
    try {
        connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'test_auth_user3'@'%'. Account is blocked for 5 second(s) (5 second(s) remaining) due to 2 consecutive failed logins."), e.getMessage())
    } 
    // sleep 5 second to unlock account
    sleep(5000)
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    // 4. test password validation
    sql """set global validate_password_policy=STRONG"""
    test {
        sql """set password for 'test_auth_user3' = password("12345")"""
        exception "Violate password validation policy: STRONG. The password must be at least 8 characters";
    }
    test {
        sql """set password for 'test_auth_user3' = password("12345678")"""
        exception "Violate password validation policy: STRONG. The password must contain at least 3 types of numbers, uppercase letters, lowercase letters and special characters.";
    }

    sql """set password for 'test_auth_user3' = password('Ab1234567^')"""
    result1 = connect(user = 'test_auth_user3', password = 'Ab1234567^', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sql """set global validate_password_policy=NONE"""

    // 5. test expire
    sql """create user test_auth_user4 identified by '12345' PASSWORD_EXPIRE INTERVAL 5 SECOND"""
    sql """grant all on *.* to test_auth_user4"""
    result1 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sleep(6000)
    try {
        connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Your password has expired. To log in you must change it using a client that supports expired passwords."), e.getMessage())
    }

    // 6. drop user and create again, new user with same name can login
    sql """drop user test_auth_user4"""
    sql """create user test_auth_user4 identified by '12345'"""
    sql """grant all on *.* to test_auth_user4"""
    result1 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    // 7. test after expire, reset password
    sql """drop user test_auth_user4"""
    sql """create user test_auth_user4 identified by '12345' PASSWORD_EXPIRE INTERVAL 5 SECOND"""
    sql """grant all on *.* to test_auth_user4"""
    result1 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sleep(6000)
    sql """set password for 'test_auth_user4' = password('123')"""
    result2 = connect(user = 'test_auth_user4', password = '123', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sleep(6000)
    try {
        connect(user = 'test_auth_user4', password = '123', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Your password has expired. To log in you must change it using a client that supports expired passwords."), e.getMessage())
    }

    // 8. test password not expiration
    sql """drop user test_auth_user4"""
    sql """create user test_auth_user4 identified by '12345'"""
    sql """grant all on *.* to test_auth_user4"""
    result1 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sleep(1000)
    result2 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    // 9. test user default database privileges
    sql """drop user if exists test_auth_user4"""
    sql """create user test_auth_user4 identified by '12345'"""
    sql """grant SELECT_PRIV on regression_test.* to test_auth_user4"""
    result1 = connect(user = 'test_auth_user4', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
        sql 'use information_schema'
        sql 'use mysql'
    }
}

