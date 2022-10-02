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

suite("test_alter_user", "account") {

    sql """drop role if exists test_auth_role1"""
    sql """drop role if exists test_auth_role2"""
    sql """drop user if exists test_auth_user1"""
    sql """drop user if exists test_auth_user2"""
    sql """drop user if exists test_auth_user3"""

    // 1. change user's default role
    sql """create role test_auth_role1"""
    sql """grant select_priv on db1.* to role 'test_auth_role1'"""
    sql """create role test_auth_role2"""
    sql """grant drop_priv on ctl.*.* to role 'test_auth_role2'"""
    
    sql """create user test_auth_user1 identified by '12345' default role 'test_auth_role1'"""
    order_qt_show_grants1 """show grants for 'test_auth_user1'"""

    sql """alter user test_auth_user1 default role 'test_auth_role2'"""
    order_qt_show_grants2 """show grants for 'test_auth_user1'"""

    sql """grant load_priv on ctl.*.* to test_auth_user1"""
    sql """grant load_priv on ctl.*.* to role 'test_auth_role2'"""

    // change user's role again
    sql """alter user test_auth_user1 default role 'test_auth_role1'"""
    order_qt_show_grants3 """show grants for 'test_auth_user1'"""
    
    // 2. test password history
    sql """set global password_history=0""" // disabled
    sql """create user test_auth_user2 identified by '12345' password_history default"""
    sql """grant all on *.* to test_auth_user2"""
    sql """alter user test_auth_user2 identified by '12345'"""
    sql """set password for 'test_auth_user2' = password('12345')"""
    
    sql """set global password_history=1""" // set to 1
    test {
        sql """alter user test_auth_user2 identified by '12345'"""
        exception "Cannot use these credentials for 'default_cluster:test_auth_user2'@'%' because they contradict the password history policy"
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
        exception "Cannot use these credentials for 'default_cluster:test_auth_user2'@'%' because they contradict the password history policy"
    }
    result1 = connect(user = 'test_auth_user2', password = 'abc123456', url = context.config.jdbcUrl) {
        sql 'select 1'
    }
    sql """set global password_history=0""" // set to disabled

    // 3. test FAILED_LOGIN_ATTEMPTS and PASSWORD_LOCK_TIME
    sql """create user test_auth_user3 identified by '12345' FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1"""
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
        assertTrue(e.getMessage().contains("Access denied for user 'default_cluster:test_auth_user3"), e.getMessage())
    } 
    try {
        connect(user = 'test_auth_user3', password = 'wrong', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'default_cluster:test_auth_user3"), e.getMessage())
    } 
    // login with correct password but also failed
    try {
        connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {}
        assertTrue(false. "should not be able to login")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Access denied for user 'default_cluster:test_auth_user3'@'%'. Account is blocked for 1 day(s) (1 day(s) remaining) due to 2 consecutive failed logins."), e.getMessage())
    } 

    // unlock user
    sql """alter user test_auth_user3 account_unlock"""
    result1 = connect(user = 'test_auth_user3', password = '12345', url = context.config.jdbcUrl) {
        sql 'select 1'
    }

    order_qt_show_proc """show proc "/auth/'default_cluster:test_auth_user3'@'%'";"""
    order_qt_show_grants """show all grants"""
}




















