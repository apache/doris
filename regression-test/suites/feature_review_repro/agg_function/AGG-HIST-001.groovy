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

suite("AGG-HIST-001") {
    sql "DROP TABLE IF EXISTS agg_hist_001"
    sql """
        CREATE TABLE agg_hist_001 (
            id INT,
            v DOUBLE
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """

    sql "INSERT INTO agg_hist_001 VALUES (1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4)"

    order_qt_linear_histogram_decimal_boundary """
        SELECT linear_histogram(v, 0.1) FROM agg_hist_001
    """

    order_qt_linear_histogram_large_near_boundary """
        SELECT linear_histogram(v, 1) FROM (
            SELECT 999999999.999999 AS v
            UNION ALL
            SELECT 1000000000.0 AS v
        ) t
    """

    order_qt_linear_histogram_near_decimal_interval """
        SELECT linear_histogram(v, 0.10000000000000002) FROM (
            SELECT 0.1 AS v
        ) t
    """

    order_qt_linear_histogram_small_nonzero_interval """
        SELECT linear_histogram(v, 1e-16) FROM (
            SELECT 1e-16 AS v
            UNION ALL
            SELECT 2e-16 AS v
        ) t
    """
}
