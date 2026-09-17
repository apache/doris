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

suite("simplify_window_frame") {
    sql "DROP TABLE IF EXISTS test_simplify_window_frame"

    sql """
        CREATE TABLE test_simplify_window_frame (
            pk INT NOT NULL,
            v INT NULL
        )
        UNIQUE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql "INSERT INTO test_simplify_window_frame VALUES (1, 7)"
    sql "SYNC"

    qt_frames_exclude_current_row """
        SELECT pk,
               SUM(v) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sum_prev,
               COUNT(*) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS count_prev,
               SUM(v) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS sum_next,
               COUNT(*) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS count_next
        FROM test_simplify_window_frame
        ORDER BY pk
    """

    qt_frames_exclude_current_row_shape """
        EXPLAIN SHAPE PLAN
        SELECT SUM(v) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS sum_prev,
               COUNT(*) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS count_next
        FROM test_simplify_window_frame
    """

    qt_frames_contain_current_row """
        SELECT pk,
               SUM(v) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS sum_cur,
               COUNT(*) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS count_centered
        FROM test_simplify_window_frame
        ORDER BY pk
    """

    qt_frames_contain_current_row_shape """
        EXPLAIN SHAPE PLAN
        SELECT SUM(v) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS sum_cur,
               COUNT(*) OVER (PARTITION BY pk ORDER BY pk
                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS count_centered
        FROM test_simplify_window_frame
    """
}
