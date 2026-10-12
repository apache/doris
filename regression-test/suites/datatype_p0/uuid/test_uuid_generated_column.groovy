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

// Checklist: B12 F03.
suite("test_uuid_generated_column", "p0") {
    // UUID generated columns are computed by the normal expression/cast pipeline.
    sql "DROP TABLE IF EXISTS uuid_generated"
    sql """
        CREATE TABLE uuid_generated (
            id INT,
            source_value STRING,
            generated_value UUID GENERATED ALWAYS AS (CAST(source_value AS UUID))
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO uuid_generated(id, source_value) VALUES
            (1, '550E8400E29B41D4A716446655440000'),
            (2, '00000000-0000-0000-0000-000000000001')
    """
    order_qt_uuid_generated """
        SELECT id, source_value, CAST(generated_value AS STRING)
        FROM uuid_generated ORDER BY id
    """
}
