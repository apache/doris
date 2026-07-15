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

suite("test_lead_lag_large_offset") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    order_qt_lag_max_int64 """
        SELECT k, LAG(k, 9223372036854775807) OVER (ORDER BY k) AS lag_big
        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
    """

    test {
        sql """
            SELECT k, SUM(k) OVER (
                ORDER BY k ROWS BETWEEN 9223372036854775808 PRECEDING AND CURRENT ROW
            ) AS sum_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "BoundOffset of ROWS WindowFrame must not exceed 9223372036854775807"
    }

    test {
        sql """
            SELECT k, LAG(k, 922337203685477580.1) OVER (ORDER BY k) AS lag_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LAG must be a constant positive integer"
    }

    test {
        sql """
            SELECT k, LEAD(k, 922337203685477580.1) OVER (ORDER BY k) AS lead_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LEAD must be a constant positive integer"
    }
}
