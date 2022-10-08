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

suite("connect_action") {
    logger.info("ok")
    def result1 = connect(user = 'admin', password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
        // execute sql with admin user
        sql 'select 99 + 1'
    }

    // if not specify <user, password, url>, it will be set to context.config.jdbc<User, Password, Url>
    //
    // user: 'root'
    // password: context.config.jdbcPassword
    // url: context.config.jdbcUrl
    def result2 = connect('root') {
        // execute sql with root user
        sql 'select 50 + 50'
    }

    assertEquals(result1, result2)
}
