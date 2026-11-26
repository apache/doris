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

suite("test_timestamptz_coalesce", "datatype_p0") {
    sql "SET time_zone = 'Asia/Shanghai'"
    sql "SET enable_strict_cast = false"
    sql "SET short_circuit_evaluation = false"

    sql "DROP TABLE IF EXISTS timestamptz_coalesce_check"
    sql """
        CREATE TABLE timestamptz_coalesce_check (
            row_id INT,
            ts_value TIMESTAMPTZ NULL
        )
        DUPLICATE KEY(row_id)
        DISTRIBUTED BY HASH(row_id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO timestamptz_coalesce_check VALUES
            (1, '2024-01-01 00:00:00+00:00'),
            (2, NULL)
    """

    order_qt_coalesce_nullable_timestamptz """
        SELECT
            row_id,
            ts_value,
            COALESCE(
                ts_value,
                CAST('2024-01-01 00:00:00+00:00' AS TIMESTAMPTZ)
            ) AS result_value
        FROM timestamptz_coalesce_check
        ORDER BY row_id
    """
}
