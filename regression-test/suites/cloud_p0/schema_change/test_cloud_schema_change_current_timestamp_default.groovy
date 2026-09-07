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

suite("test_cloud_schema_change_current_timestamp_default", "p0") {
    sql "DROP TABLE IF EXISTS test_cloud_schema_change_current_timestamp_default"
    sql """
        CREATE TABLE test_cloud_schema_change_current_timestamp_default (
            id INT NOT NULL,
            value_col INT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true"
        )
    """

    sql "INSERT INTO test_cloud_schema_change_current_timestamp_default VALUES (1, 10)"

    // Adding a key column forces a heavy schema change and creates cloud shadow tablets.
    sql """
        ALTER TABLE test_cloud_schema_change_current_timestamp_default
        ADD COLUMN created_at DATETIMEV2(6) KEY DEFAULT CURRENT_TIMESTAMP(6) AFTER id
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_cloud_schema_change_current_timestamp_default'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    Thread.sleep(2000)
    sql """
        INSERT INTO test_cloud_schema_change_current_timestamp_default (id, value_col)
        VALUES (2, 20)
    """
    sql "sync"

    order_qt_backfill_is_frozen_literal """
        SELECT id, created_at > '2000-01-01 00:00:00.000000' AS valid_default
        FROM test_cloud_schema_change_current_timestamp_default
        ORDER BY id
    """
    order_qt_default_expression_is_preserved """
        SELECT COUNT(DISTINCT created_at)
        FROM test_cloud_schema_change_current_timestamp_default
    """
}
