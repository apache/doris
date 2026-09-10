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

suite("merge_one_row_relation_into_union") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        EXPLAIN SHAPE PLAN SELECT v
        FROM (
            SELECT CAST('unused' AS CHAR(6)) AS k, CAST('alpha' AS CHAR(6)) AS v
            UNION ALL
            SELECT CAST('unused' AS CHAR(6)), CAST('beta' AS CHAR(6))
        ) u
        GROUP BY v
    """

    test {
        sql """
            SELECT 1 AS c
            UNION ALL
            SELECT c FROM (SELECT 2 AS c) s GROUP BY c
            ORDER BY c
        """
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            assertEquals("[[1], [2]]", result.toString())
        }
    }

    test {
        sql """
            SELECT 1 AS c
            UNION ALL
            SELECT DISTINCT c FROM (SELECT 2 AS c) s
            ORDER BY c
        """
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            assertEquals("[[1], [2]]", result.toString())
        }
    }

    test {
        sql """
            SELECT c FROM (SELECT 2 AS c) s GROUP BY c
            UNION ALL
            SELECT 1 AS c
            ORDER BY c
        """
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            assertEquals("[[1], [2]]", result.toString())
        }
    }

    test {
        sql """
            SELECT 1 AS c UNION ALL SELECT 1 AS c UNION ALL SELECT 1 AS c ORDER BY c
        """
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            assertEquals("[[1], [1], [1]]", result.toString())
        }
    }
}
