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

suite("test_show_index", "query,arrow_flight_sql") {
    String table_name = "table_for_index_test";
    String index_name = "index_for_index_test";
    try {
        // Create a table for testing
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                id INT COMMENT "Primary key",
                age INT COMMENT "Age field",
                name STRING COMMENT "Name field"
            )
            DISTRIBUTED BY HASH(id) BUCKETS 5
            PROPERTIES ("replication_num" = "1");
        """

        // Create an index based on the created table
        sql """
            CREATE INDEX IF NOT EXISTS ${index_name}
            ON ${table_name} (age) USING INVERTED COMMENT 'index for test';
        """

        // Execute the SHOW INDEX command
        checkNereidsExecute("""show index from `${table_name}`;""")
        qt_cmd("""show index from `${table_name}`;""")
    } finally {
        // Drop the index and table after testing
        try_sql("DROP INDEX IF EXISTS `${index_name}` ON `${table_name}`")
        try_sql("DROP TABLE IF EXISTS `${table_name}`")
    }
}
