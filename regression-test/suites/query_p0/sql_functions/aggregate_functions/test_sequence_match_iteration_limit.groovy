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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite("test_sequence_match_iteration_limit") {
    sql "SET query_timeout = 120"
    sql "DROP TABLE IF EXISTS sequence_match_iteration_limit_test"
    sql """
        CREATE TABLE sequence_match_iteration_limit_test (
            id BIGINT NOT NULL,
            ts DATETIMEV2(0) NOT NULL,
            evt TINYINT NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO sequence_match_iteration_limit_test
        SELECT number,
               IF(number < 1000001, CAST('2026-01-01 00:00:00' AS DATETIMEV2(0)),
                                    CAST('2026-01-01 00:00:01' AS DATETIMEV2(0))),
               CAST(IF(number < 1000001, 1, 2) AS TINYINT)
        FROM numbers('number' = '1000002')
    """

    // A single early event 1 and the final event 2 form one valid match.
    qt_small_match """
        SELECT sequence_match('(?1)(?t>=1)(?2)', ts, evt = 1, evt = 2),
               sequence_count('(?1)(?t>=1)(?2)', ts, evt = 1, evt = 2)
        FROM sequence_match_iteration_limit_test WHERE id IN (0, 1000001)
    """
    qt_small_no_match """
        SELECT sequence_match('(?1)(?t>=2)(?2)', ts, evt = 1, evt = 2),
               sequence_count('(?1)(?t>=2)(?2)', ts, evt = 1, evt = 2)
        FROM sequence_match_iteration_limit_test WHERE id IN (0, 1000001)
    """

    // The final event must not be reported as absent when the search budget is exhausted.
    test {
        sql """SELECT sequence_match('(?1)(?t>=1)(?2)', ts, evt = 1, evt = 2)
               FROM sequence_match_iteration_limit_test"""
        exception "exceeded max iterations"
    }
    test {
        sql """SELECT sequence_count('(?1)(?t>=1)(?2)', ts, evt = 1, evt = 2)
               FROM sequence_match_iteration_limit_test"""
        exception "exceeded max iterations"
    }
}
