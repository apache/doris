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

// Checklist: A04 B05 B12 E04.
suite("test_uuid_dynamic_defaults", "p0") {
    // All UUID generator names can be used as per-row dynamic defaults.
    sql "DROP TABLE IF EXISTS uuid_function_defaults"
    sql """
        CREATE TABLE uuid_function_defaults (
            id INT NOT NULL,
            v4 UUID NOT NULL DEFAULT UUID_V4(),
            v4_alias1 UUID NOT NULL DEFAULT GENERATE_UUID_V4(),
            v4_alias2 UUID NOT NULL DEFAULT GENERATEUUIDV4(),
            v7 UUID NOT NULL DEFAULT UUID_V7(),
            v7_alias1 UUID NOT NULL DEFAULT GENERATE_UUID_V7(),
            v7_alias2 UUID NOT NULL DEFAULT GENERATEUUIDV7()
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO uuid_function_defaults(id) VALUES (1), (2), (3)"
    qt_uuid_function_defaults """
        SELECT
            MIN(UUID_VERSION(v4)), MIN(UUID_VERSION(v4_alias1)), MIN(UUID_VERSION(v4_alias2)),
            MIN(UUID_VERSION(v7)), MIN(UUID_VERSION(v7_alias1)), MIN(UUID_VERSION(v7_alias2)),
            COUNT(DISTINCT v4), COUNT(DISTINCT v4_alias1), COUNT(DISTINCT v4_alias2),
            COUNT(DISTINCT v7), COUNT(DISTINCT v7_alias1), COUNT(DISTINCT v7_alias2)
        FROM uuid_function_defaults
    """
    test {
        sql """
            CREATE TABLE uuid_function_default_wrong_type (
                id INT DEFAULT UUID_V4()
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "Types other than UUID"
    }
}
