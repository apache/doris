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

suite("test_timestamptz_cast_gap_partition_prune", "p0") {
    sql "DROP TABLE IF EXISTS timestamptz_cast_gap_partition_prune"
    sql """
        CREATE TABLE timestamptz_cast_gap_partition_prune (dt DATETIME(6))
        DUPLICATE KEY(dt)
        PARTITION BY RANGE(dt) (
            PARTITION p VALUES [('2024-03-10 02:30:00.000000'), ('2024-03-10 03:00:00.100000'))
        )
        DISTRIBUTED BY HASH(dt) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql "INSERT INTO timestamptz_cast_gap_partition_prune VALUES ('2024-03-10 02:40:00.900000')"

    def originalTimeZone = sql "SELECT @@time_zone"
    try {
        sql "SET time_zone = 'America/New_York'"
        // The row converts to 07:40:00.9 UTC. The upper bound converts to 07:00:00.1 UTC.
        // The predicate is true for this row.
        order_qt_gap_partition """
            SELECT dt, CAST(dt AS TIMESTAMPTZ(6))
            FROM timestamptz_cast_gap_partition_prune
            WHERE CAST(dt AS TIMESTAMPTZ(6)) > CAST('2024-03-10 07:00:00.500000+00:00' AS TIMESTAMPTZ(6))
        """
    } finally {
        sql "SET time_zone = '${originalTimeZone[0][0]}'"
    }
}
