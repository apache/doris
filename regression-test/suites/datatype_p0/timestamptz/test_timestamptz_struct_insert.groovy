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

suite("test_timestamptz_struct_insert") {
    sql "DROP TABLE IF EXISTS test_timestamptz_struct_insert"
    sql """
        CREATE TABLE test_timestamptz_struct_insert (
            id INT,
            st STRUCT<a:TIMESTAMPTZ(6)>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "SET enable_nereids_planner = true"
    sql "SET time_zone = '+00:00'"

    sql """
        INSERT INTO test_timestamptz_struct_insert
        VALUES (
            1,
            NAMED_STRUCT(
                'a',
                CAST('2024-03-30 20:45:00 -03:30' AS TIMESTAMPTZ(6))
            )
        )
    """

    order_qt_select "SELECT id, st.a FROM test_timestamptz_struct_insert ORDER BY id"
}
