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

suite("max_msg_size_of_result_receiver") {
    def MESSAGE_SIZE_BASE=1000*1000; // 1MB
    def DEFAULT_MAX_MESSAGE_SIZE = 104000000; // 104MB
    def table_name = "max_msg_size_of_result_receiver"
    sql """
        DROP TABLE  IF EXISTS ${table_name}
    """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (id int, str string)
        ENGINE=OLAP DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num"="1")
    """
    sql """set max_msg_size_of_result_receiver=90000;""" // so the test of repeat("a", 80000) could pass, and repeat("a", 100000) will be failed
    sql """
        INSERT INTO ${table_name} VALUES (104, repeat("a", 80000))
    """

    sql """
        INSERT INTO ${table_name} VALUES (105, repeat("a", 100000))
    """

    def with_exception = false
    try {
        sql "SELECT * FROM ${table_name} WHERE id = 104"
    } catch (Exception e) {
        with_exception = true
    }
    assertEquals(with_exception, false)
    
    test {
        sql """SELECT * FROM ${table_name} WHERE id = 105;"""
        exception "MaxMessageSize reached, try increase max_msg_size_of_result_receiver"
    }

    try {
        sql "SELECT /*+SET_VAR(max_msg_size_of_result_receiver=${DEFAULT_MAX_MESSAGE_SIZE * 2})*/ * FROM ${table_name} WHERE id = 105"
    } catch (Exception e) {
        with_exception = true
    
    }
    assertEquals(with_exception, false)

}