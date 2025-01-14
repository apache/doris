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

suite("test_show_create_materialized_view", "query,arrow_flight_sql") {
    String tableName = "table_for_mv_test";
    String mvName = "mv_show_create_materialized_view";
    try {  
        sql """DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT COMMENT "Primary key",
                name STRING COMMENT "Name field",
                value DECIMAL(10, 2) COMMENT "Value field"
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 5
            PROPERTIES ("replication_num" = "1");
        """
        
        createMV("""CREATE MATERIALIZED VIEW ${mvName} AS
            SELECT id, name, SUM(value) AS total_value
            FROM ${tableName}
            GROUP BY id, name;
        """)
        
        checkNereidsExecute("""SHOW CREATE MATERIALIZED VIEW ${mvName} ON ${tableName};""")
        qt_cmd("""SHOW CREATE MATERIALIZED VIEW ${mvName} ON ${tableName};""")

    } finally {
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
        sql """DROP TABLE IF EXISTS ${tableName}"""
    }
}
