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

suite("test_show_statistic_proc", "nonConcurrent") {

    sql """drop user if exists test_show_statistic_proc_user1"""

    sql """create user test_show_statistic_proc_user1 identified by '12345'"""

    sql """grant ADMIN_PRIV on *.*.* to test_show_statistic_proc_user1"""

    sql """drop database if exists test_statistic_proc_db"""
    sql """create database test_statistic_proc_db"""

    def result1 = connect(user = 'test_show_statistic_proc_user1', password = '12345', url = context.config.jdbcUrl) {
        sql """ show proc '/statistic' """
    }
    def result2 = connect(user = 'test_show_statistic_proc_user1', password = '12345', url = context.config.jdbcUrl) {
        sql """ show databases """
    }
    assertEquals(result1.size(), result2.size())
    assertEquals(result1[result1.size() - 1][1].toInteger(), result2.size() - 1)
    def containsTargetDb = false
    result1.each {  row ->
        if (row[1] == 'test_statistic_proc_db') {
             containsTargetDb = true
             return
        }
    }
    assertTrue(containsTargetDb)
}

