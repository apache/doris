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

suite("test_account") {
    // test comment
    def user = "test_account_comment_user";
    sql """drop user if exists ${user}"""
    // create user with comment
    sql """create user ${user} comment 'test_account_comment_user_comment_create'"""
    def user_create = sql "show grants for ${user}"
    logger.info("user_create: " + user_create.toString())
    assertTrue(user_create.toString().contains("test_account_comment_user_comment_create"))
    // alter user comment
    sql """alter user ${user} comment 'test_account_comment_user_comment_alter'"""
    def user_alter = sql "show grants for ${user}"
    logger.info("user_alter: " + user_alter.toString())
    assertTrue(user_alter.toString().contains("test_account_comment_user_comment_alter"))
    // drop user
    sql """drop user if exists ${user}"""
    try {
        sql "show grants for ${user}"
        fail()
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains('not exist'))
    }
}
