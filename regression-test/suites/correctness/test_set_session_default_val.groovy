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

suite("test_set_session_default_val") {
    def default_timeout = sql """show variables where variable_name = 'insert_timeout';"""
    sql """set insert_timeout=3000;"""
    sql """set session insert_timeout=${default_timeout[0][1]};"""
    def session_timeout = sql """show variables where variable_name = 'insert_timeout';"""
    assertEquals(default_timeout, session_timeout)

    def default_query_timeout = sql """show variables where variable_name = 'query_timeout';"""
    def default_max_execute_timeout = sql """show variables where variable_name = 'max_execution_time';"""

    sql """set query_timeout=2;"""
    def query_timeout = sql """show variables where variable_name = 'query_timeout';"""
    def max_execute_timeout = sql """show variables where variable_name = 'max_execution_time';"""
    assertEquals(query_timeout[0][1], "2")
    assertEquals(max_execute_timeout[0][1], default_max_execute_timeout[0][1])

    sql """set max_execution_time=3000;"""
    def query_timeout2 = sql """show variables where variable_name = 'query_timeout';"""
    def max_execute_timeout2 = sql """show variables where variable_name = 'max_execution_time';"""
    assertEquals(query_timeout2[0][1], "3")
    assertEquals(max_execute_timeout2[0][1], "3000")
}
