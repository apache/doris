/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Six rows cover NULL in either argument, both arguments, and a valid row after a NULL.
// Data is stored before each query. The queries therefore exercise the column reader.
suite("test_nullable_scan_short_runs") {
    sql "DROP TABLE IF EXISTS nullable_scan_short_runs"
    sql """
        CREATE TABLE nullable_scan_short_runs (
            id INT NOT NULL,
            x DOUBLE NULL,
            w DOUBLE NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO nullable_scan_short_runs VALUES
            (0, 1, 1), (1, NULL, 2), (2, 3, NULL),
            (3, 4, 2), (4, NULL, NULL), (5, 6, 3)
    """
    // Read the stored values directly, so aggregate NULL skipping cannot hide a bad null map.
    qt_values "SELECT * FROM nullable_scan_short_runs ORDER BY id"
    qt_weighted "SELECT avg_weighted(x, w) FROM nullable_scan_short_runs"
    // Group 1 has only NULL contributions. Its result must remain NULL.
    qt_grouped """
        SELECT id % 3 AS g, avg_weighted(x, w)
        FROM nullable_scan_short_runs GROUP BY g ORDER BY g
    """
}
