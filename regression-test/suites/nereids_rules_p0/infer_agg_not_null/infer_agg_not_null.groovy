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

suite("infer_agg_not_null") {
    sql "DROP TABLE IF EXISTS infer_agg_not_null_left"
    sql "DROP TABLE IF EXISTS infer_agg_not_null_right"

    sql """
        CREATE TABLE infer_agg_not_null_left (
            k INT NOT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE infer_agg_not_null_right (
            k INT NOT NULL,
            v INT NULL
        ) DUPLICATE KEY(k, v)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO infer_agg_not_null_left VALUES (1), (2), (3)"
    sql "INSERT INTO infer_agg_not_null_right VALUES (2, 9), (3, NULL)"

    order_qt_count_boolean_argument """
        SELECT COUNT(r.v IS NOT NULL)
        FROM infer_agg_not_null_left l
        LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
    """

    order_qt_count_nullable_slot """
        SELECT COUNT(r.v)
        FROM infer_agg_not_null_left l
        LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
    """

    order_qt_count_coalesce_argument """
        SELECT COUNT(COALESCE(r.v, 0))
        FROM infer_agg_not_null_left l
        LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
    """

    order_qt_mixed_count_array_agg """
        SELECT COUNT(r.v), ARRAY_SIZE(ARRAY_AGG(r.v))
        FROM infer_agg_not_null_left l
        LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
    """

    explain {
        sql """
            SHAPE PLAN
            SELECT COUNT(r.v IS NOT NULL)
            FROM infer_agg_not_null_left l
            LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
        """
        contains "LEFT_OUTER_JOIN"
    }

    explain {
        sql """
            SHAPE PLAN
            SELECT COUNT(r.v)
            FROM infer_agg_not_null_left l
            LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
        """
        contains "INNER_JOIN"
    }

    explain {
        sql """
            SHAPE PLAN
            SELECT COUNT(COALESCE(r.v, 0))
            FROM infer_agg_not_null_left l
            LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
        """
        contains "LEFT_OUTER_JOIN"
    }

    explain {
        sql """
            SHAPE PLAN
            SELECT COUNT(r.v), ARRAY_SIZE(ARRAY_AGG(r.v))
            FROM infer_agg_not_null_left l
            LEFT JOIN infer_agg_not_null_right r ON l.k = r.k
        """
        contains "LEFT_OUTER_JOIN"
    }
}
