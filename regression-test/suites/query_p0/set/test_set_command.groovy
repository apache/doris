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

suite("test_set_command") {
    // setSystemVariable
    def default_value = sql """show variables where variable_name = 'insert_timeout';"""
    sql """set insert_timeout=97531;"""
    def modified_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertTrue(modified_value.toString().contains('97531'))
    sql """set insert_timeout=DEFAULT;"""
    def restored_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertEquals(default_value, restored_value)

    default_value = sql """show variables where variable_name = 'insert_timeout';"""
    sql """set @@insert_timeout=97531;"""
    modified_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertTrue(modified_value.toString().contains('97531'))
    sql """set @@insert_timeout=DEFAULT;"""
    restored_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertEquals(default_value, restored_value)

    default_value = sql """show variables where variable_name = 'insert_timeout';"""
    sql """set @@session.insert_timeout=97531;"""
    modified_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertTrue(modified_value.toString().contains('97531'))
    sql """set @@session.insert_timeout=DEFAULT;"""
    restored_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertEquals(default_value, restored_value)

    // setVariableWithType
    default_value = sql """show variables where variable_name = 'insert_timeout';"""
    sql """set session insert_timeout=97531;"""
    modified_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertTrue(modified_value.toString().contains('97531'))
    sql """set session insert_timeout=DEFAULT;"""
    restored_value = sql """show variables where variable_name = 'insert_timeout';"""
    assertEquals(default_value, restored_value)

    // setNames do nothing
    sql """set names = utf8;"""

    // setCollate do nothing
    sql """set names default collate utf_8_ci;"""

    // setTransaction do nothing
    sql """set transaction read only;"""
    sql """set transaction read write;"""

    // setCharset do nothing
    sql """set charset utf8;"""
    sql """set charset default;"""
}