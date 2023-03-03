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

suite("dict_with_null", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test_dict_with_null"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          c_int INT,
          c_string VARCHAR(10)
        )
        DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
    """

    // Here insert all rows in one statement to make sure they are in one same segment.
    // Insert 100 + 1 rows because `SegmentIterator` will read 100 rows in the first time.
    // The first 100 rows are all null to make no record be inserted into dictionary at the first read time.
    def insert_sql = "insert into ${tableName} values "
    for (int i in 1..100) {
        if (i != 1) {
            insert_sql += ", "
        }
        insert_sql += "(${i}, null)"
    }
    insert_sql += ", (101, 'abc')"

    sql insert_sql
    sql "select * from test_dict_with_null where c_string > '0'"
}