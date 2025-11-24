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

suite("test_show_txn_error_msg") {
    def unique_label = "label_a_" + UUID.randomUUID().toString().replaceAll("-", "")
    sql """ DROP TABLE IF EXISTS test """
    sql """ CREATE TABLE test (id INT, x INT) PROPERTIES("replication_num" = "1") """
    sql """ INSERT INTO test WITH LABEL ${unique_label} VALUES (1, 2) """

    test {
        sql """ SHOW TRANSACTION WHERE label = '${unique_label}' """
        check { result, exception, startTime, endTime ->
            assertTrue(result.size() > 0, "Expected to find transaction with label ${unique_label}")
        }
    }

    // transaction with label 'label_b' does not exist
    test {
        sql """ SHOW TRANSACTION WHERE label = 'label_b' """
        exception("errCode = 2, detailMessage = transaction with label label_b does not exist")
    }

    sql """ DROP TABLE IF EXISTS test """
}