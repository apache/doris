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

suite("test_having_project") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS t
       """
    sql """
        create table t(id smallint) distributed by random properties('replication_num'='1');
    """

    qt_having_project_1 """
        SELECT 1 AS c1 FROM t HAVING count(1) >= 0
    """

    qt_having_project_2 """
        SELECT 1 AS c1 FROM t HAVING count(1) > 0
    """

    qt_having_map_lambda_local_slots """
        SELECT id, COUNT(*) AS n
        FROM (SELECT 1 id UNION ALL SELECT 1 id) input
        GROUP BY id
        HAVING map_exists((k, v) -> v > 1, map(1, COUNT(*)))
        ORDER BY id
    """

    qt_having_array_lambda_local_slots """
        SELECT id, COUNT(*) AS n
        FROM (SELECT 1 id UNION ALL SELECT 1 id) input
        GROUP BY id
        HAVING array_match_any(array_map(x -> x > 1, array(COUNT(*))))
        ORDER BY id
    """

    test {
        sql """
            SELECT id, COUNT(*) AS n
            FROM (
                SELECT 1 id, 1 AS ungrouped_col
                UNION ALL
                SELECT 1 id, 2 AS ungrouped_col
            ) input
            GROUP BY id
            HAVING array_match_any(array_map(x -> x > 1, array(COUNT(*) + ungrouped_col)))
        """
        exception "HAVING expression 'ungrouped_col' must appear in the GROUP BY clause or be used in an aggregate function"
    }

    test {
        sql "SELECT 1 AS c1 FROM t HAVING count(1) > 0 OR c1 IS NOT NULL"
        exception "HAVING expression 'c1' must appear in the GROUP BY clause or be used in an aggregate function"
    }
}
