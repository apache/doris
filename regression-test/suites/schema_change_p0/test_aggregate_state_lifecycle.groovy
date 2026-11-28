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

suite("test_aggregate_state_lifecycle", "p0") {
    sql "DROP TABLE IF EXISTS test_aggregate_state_lifecycle"
    sql """
        CREATE TABLE test_aggregate_state_lifecycle (
            k INT NOT NULL,
            subkey INT NOT NULL,
            v1 VARCHAR(64) MIN,
            v2 VARCHAR(64) MAX
        ) AGGREGATE KEY(k, subkey)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "light_schema_change" = "false")
    """
    sql "INSERT INTO test_aggregate_state_lifecycle VALUES (1, 1, 'alpha', 'one'), (2, 1, 'beta', 'two')"
    sql "INSERT INTO test_aggregate_state_lifecycle VALUES (1, 2, 'alpha', 'one'), (2, 2, 'beta', 'two')"
    order_qt_before "SELECT k, subkey, v1, v2 FROM test_aggregate_state_lifecycle ORDER BY k, subkey"

    sql "ALTER TABLE test_aggregate_state_lifecycle DROP COLUMN subkey"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE IndexName='test_aggregate_state_lifecycle' ORDER BY createtime DESC LIMIT 1"
        time 600
    }
    order_qt_after "SELECT k, v1, v2 FROM test_aggregate_state_lifecycle ORDER BY k"
}
