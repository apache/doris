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

// Checklist: A08 C04 E07.
suite("test_uuid_aggregate_state", "p0") {
    // Aggregate-state serialization and merge preserve the UUID payload.
    sql "SET enable_agg_state = true"
    sql "DROP TABLE IF EXISTS uuid_agg_state"
    sql """
        CREATE TABLE uuid_agg_state (
            id INT NOT NULL,
            state AGG_STATE<MAX(UUID NULL)> GENERIC
        ) AGGREGATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO uuid_agg_state VALUES
            (1, MAX_STATE(CAST('00000000-0000-0000-0000-000000000001' AS UUID))),
            (1, MAX_STATE(CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)))
    """
    order_qt_uuid_agg_state """
        SELECT id, CAST(MAX_MERGE(state) AS STRING)
        FROM uuid_agg_state GROUP BY id ORDER BY id
    """
}
