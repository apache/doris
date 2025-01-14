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

import org.junit.Assert;

suite("test_grant_priv") {
    def user1 = 'test_grant_priv_user1'
    def user2 = 'test_grant_priv_user2'
    def role1 = 'test_grant_priv_role1'
    def pwd = '123456'
    def dbName = 'test_grant_priv_db'
    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

    sql """drop user if exists ${user1}"""
    sql """drop user if exists ${user2}"""
    sql """drop role if exists ${role1}"""
    sql """DROP DATABASE IF EXISTS ${dbName}"""

    sql """CREATE DATABASE ${dbName}"""
    sql """CREATE ROLE ${role1}"""
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'"""

    // test only have select_priv, can not grant to other user
    sql """grant select_priv on ${dbName}.* to ${user1}"""
    connect(user1, "${pwd}", url) {
        try {
            sql """grant select_priv on ${dbName}.* to ${user2}"""
            Assert.fail("can not grant to other user");
        } catch (Exception e) {
            log.info(e.getMessage())
        }
    }

    // test both have select_priv and grant_priv , can grant to other user
    sql """grant grant_priv on ${dbName}.* to ${user1}"""
    connect(user1, "${pwd}", url) {
        try {
            sql """grant select_priv on ${dbName}.* to ${user2}"""
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // test have grant_priv,but not have load_priv, can not grant load_priv to other user
        try {
            sql """grant load_priv on ${dbName}.* to ${user2}"""
            Assert.fail("can not grant to other user");
        } catch (Exception e) {
            log.info(e.getMessage())
        }
        // test have grant_priv, can not grant role to other user
        try {
            sql """grant '${role1}' to ${user2}"""
            Assert.fail("can not grant to other user");
        } catch (Exception e) {
            log.info(e.getMessage())
        }
    }

    // test have global grant_priv, can grant role to other user
    sql """grant grant_priv on *.* to ${user1}"""
    try {
        sql """grant '${role1}' to ${user2}"""
    } catch (Exception e) {
        Assert.fail(e.getMessage());
    }

    sql """drop user if exists ${user1}"""
    sql """drop user if exists ${user2}"""
    sql """drop role if exists ${role1}"""
    sql """DROP DATABASE IF EXISTS ${dbName}"""
}
