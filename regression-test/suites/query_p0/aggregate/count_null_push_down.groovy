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

suite("count_null_push_down") {
    def tableName = "count_null_push_down_test"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT NOT NULL,
                value INT NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    
    sql """ INSERT INTO ${tableName} VALUES (1, 1), (2, 2), (3, null), (4, 4), (5, null) """

    // Test COUNT(column) on nullable column - should use COUNT_NULL push down
    qt_count_null """ SELECT COUNT(value) FROM ${tableName} """
    
    // Test COUNT(*) - should use COUNT push down
    qt_count_star """ SELECT COUNT(*) FROM ${tableName} """
    
    // Test COUNT on non-nullable column - should use COUNT push down
    qt_count_non_null """ SELECT COUNT(id) FROM ${tableName} """
}
