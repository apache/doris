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

suite("test_txn_values_partial_update") {
    sql "DROP TABLE IF EXISTS txn_values_partial_update"
    sql """
        CREATE TABLE txn_values_partial_update (
            k INT NOT NULL,
            v1 INT NULL,
            v2 INT NULL
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "INSERT INTO txn_values_partial_update VALUES (1, 100, 1000), (2, 200, 2000)"
    sql "SET enable_unique_key_partial_update = true"
    try {
        // Both partial updates fail before initializing the VALUES load.
        sql "BEGIN"
        test {
            sql "INSERT INTO txn_values_partial_update(k, v1) VALUES (1, 111), (2, 222)"
            exception "Partial update is not supported for INSERT INTO VALUES in an explicit transaction"
        }
        test {
            sql "INSERT INTO txn_values_partial_update(k, v2) VALUES (1, 1111), (2, 2222)"
            exception "Partial update is not supported for INSERT INTO VALUES in an explicit transaction"
        }
        sql "COMMIT"
        order_qt_rejected_first "SELECT * FROM txn_values_partial_update"

        // A rejected first statement must not prevent a subsequent full-row insert.
        sql "BEGIN"
        test {
            sql "INSERT INTO txn_values_partial_update(k, v1) VALUES (1, 111)"
            exception "Partial update is not supported for INSERT INTO VALUES in an explicit transaction"
        }
        sql "INSERT INTO txn_values_partial_update VALUES (3, 300, 3000)"
        // Reject a partial update after the stream has already been initialized.
        test {
            sql "INSERT INTO txn_values_partial_update(k, v2) VALUES (3, 3333)"
            exception "Partial update is not supported for INSERT INTO VALUES in an explicit transaction"
        }
        sql "INSERT INTO txn_values_partial_update(k, v1, v2) VALUES (4, 400, 4000)"
        sql "COMMIT"
        order_qt_commit_full_rows "SELECT * FROM txn_values_partial_update"

        sql "BEGIN"
        sql "INSERT INTO txn_values_partial_update VALUES (5, 500, 5000)"
        test {
            sql "INSERT INTO txn_values_partial_update(k, v2) VALUES (1, 1111)"
            exception "Partial update is not supported for INSERT INTO VALUES in an explicit transaction"
        }
        sql "ROLLBACK"
        order_qt_rollback "SELECT * FROM txn_values_partial_update"

        // Autocommit VALUES and transactional SELECT retain partial-update support.
        sql "INSERT INTO txn_values_partial_update(k, v1) VALUES (1, 111)"
        sql "INSERT INTO txn_values_partial_update(k, v2) VALUES (1, 1111)"
        order_qt_autocommit "SELECT * FROM txn_values_partial_update"

        sql "BEGIN"
        sql "INSERT INTO txn_values_partial_update(k, v1) SELECT 2, 222"
        sql "INSERT INTO txn_values_partial_update(k, v2) SELECT 2, 2222"
        sql "COMMIT"
        order_qt_insert_select "SELECT * FROM txn_values_partial_update"
    } finally {
        sql "ROLLBACK"
        sql "SET enable_unique_key_partial_update = false"
    }
}
