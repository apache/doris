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

suite("test_insert_table_with_dump_nereids_memo") {

    def testTable = "test_insert_table_with_dump_nereids_memo"
    // Clean up any existing table with the same name
    sql "DROP TABLE IF EXISTS ${testTable}"

    // Create table with with test 'dump_nereids_memo' SessionVariable
    // set dump_nereids_memo to true
    sql "set dump_nereids_memo = true"

    // Verify that the variable is set correctly
    def result = sql "SHOW VARIABLES LIKE 'dump_nereids_memo'"
    assertTrue(result[0][1] == "true")

    sql """
    CREATE TABLE ${testTable}
    (
        id int,
        name string
    )
    COMMENT "test table with dump_nereids_memo"
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    // Insert data into the table
    sql "INSERT INTO ${testTable} VALUES (1, 'hello'), (2, 'world')"

    // Synchronize to ensure data is visible
    sql "SYNC"

    // Verify that data was inserted correctly
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // Clean up after the test
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql "set dump_nereids_memo = false"

}