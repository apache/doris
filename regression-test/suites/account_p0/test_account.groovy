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
suite("test_account") {
    // todo: test account management, such as role, user, grant, revoke ...
    sql "show roles"

    try {
        sql "show grants for 'non_existent_user_1'"
        fail()
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains('not exist'))
    }

    // test comment
    def user = "test_account_comment_user";
    sql """drop user if exists ${user}"""
    sql """create user ${user} comment 'test_account_comment_user_comment_create'"""
    qt_create "show grants for ${user}"
    sql """alter user ${user} comment 'test_account_comment_user_comment_alter'"""
    qt_alter "show grants for ${user}"
    sql """drop user if exists ${user}"""
}
