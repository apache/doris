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

suite("test_show_backends", "show") {

    sql """drop user if exists test_show_backends_user1"""

    sql """create user test_show_backends_user1 identified by '12345'"""

    sql """grant ADMIN_PRIV on *.*.* to test_show_backends_user1"""

    result1 = connect(user = 'test_show_backends_user1', password = '12345', url = context.config.jdbcUrl) {
        sql 'show backends'
    }
    println result1.toString()
    result2 = connect(user = 'test_show_backends_user1', password = '12345', url = context.config.jdbcUrl) {
            sql 'show proc \'\/backends\''
        }
    println result2.toString()
    assertEquals(result1[0][0],result2[0][0])
    assertEquals(result1[0][1],result2[0][1])
    assertEquals(result1[0][2],result2[0][2])
    assertEquals(result1[0][3],result2[0][3])
    assertEquals(result1[0][4],result2[0][4])
    assertEquals(result1[0][5],result2[0][5])
    assertEquals(result1[0][6],result2[0][6])
}

