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

suite("test_partial_update_schema_change_current_timestamp_default", "p0") {
    sql "DROP TABLE IF EXISTS test_partial_update_schema_change_current_timestamp_default"
    sql """
        CREATE TABLE test_partial_update_schema_change_current_timestamp_default (
            id INT NOT NULL,
            value_col INT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql "INSERT INTO test_partial_update_schema_change_current_timestamp_default VALUES (1, 10)"
    sql """
        ALTER TABLE test_partial_update_schema_change_current_timestamp_default
        ADD COLUMN created_at DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP(6)
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_partial_update_schema_change_current_timestamp_default'
            ORDER BY CreateTime DESC LIMIT 1
        """
    }

    Thread.sleep(2000)
    sql "SET enable_unique_key_partial_update = true"
    sql "SET enable_insert_strict = false"
    sql """
        INSERT INTO test_partial_update_schema_change_current_timestamp_default (id, value_col)
        VALUES (2, 20)
    """
    sql "sync"

    order_qt_distinct_default_count """
        SELECT COUNT(DISTINCT created_at) AS distinct_default_count
        FROM test_partial_update_schema_change_current_timestamp_default
    """
}
