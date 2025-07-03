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

suite("test_show_create_view", "query,arrow_flight_sql") {
    String view_name = "view_show_create_view";
    String table_name = "table_for_view_test";
    try {
        // Create a table for testing
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                id INT COMMENT "Primary key",
                name STRING COMMENT "Name field"
            )
            DISTRIBUTED BY HASH(id) BUCKETS 5
            PROPERTIES ("replication_num" = "1");
        """

        // Create a view based on the created table
        sql """
            CREATE VIEW IF NOT EXISTS ${view_name} AS
            SELECT id, name FROM ${table_name}
        """
        
        // Execute the SHOW CREATE VIEW command
        checkNereidsExecute("""show create view `${view_name}`;""")
        qt_cmd("""show create view `${view_name}`;""")
    } finally {
        // Drop the view and table after testing
        try_sql("DROP VIEW IF EXISTS `${view_name}`")
        try_sql("DROP TABLE IF EXISTS `${table_name}`")
    }

    // Additional case: Create another view based on a different table
    String view_name_2 = "view_show_create_view_2";
    String table_name_2 = "table_for_view_test_2";
    try {
        // Create another table for testing
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name_2} (
                `key_field` INT COMMENT "Key field",
                `value` STRING COMMENT "Value field"
            )
            DISTRIBUTED BY HASH(key_field) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        // Create a view based on the new table
        sql """
            CREATE VIEW IF NOT EXISTS ${view_name_2} AS
            SELECT key_field, value FROM ${table_name_2}
        """
        
        // Execute the SHOW CREATE VIEW command for the new view
        checkNereidsExecute("""show create view `${view_name_2}`;""")
        qt_cmd("""show create view `${view_name_2}`;""")

    } finally {
        // Drop the view and table after testing
        try_sql("DROP VIEW IF EXISTS `${view_name_2}`")
        try_sql("DROP TABLE IF EXISTS `${table_name_2}`")
    }
}
