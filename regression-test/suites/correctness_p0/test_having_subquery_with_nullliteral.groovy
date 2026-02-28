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

suite("test_having_subquery_with_nullliteral") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists test_having_subquery"

    sql """
    CREATE TABLE test_having_subquery (
            id INT NOT NULL,
            name VARCHAR(25) NOT NULL,
            time VARCHAR(40) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql "INSERT INTO test_having_subquery VALUES(1,'jack','202501')"
    sql "INSERT INTO test_having_subquery VALUES(2,'rose','202501')"
    sql "INSERT INTO test_having_subquery VALUES(3,'tom','202502')"

    qt_having_count_distinct_subquery """
    SELECT time
        FROM test_having_subquery
        GROUP BY time
        HAVING COUNT(DISTINCT time) = (
            SELECT COUNT(DISTINCT time)
            FROM test_having_subquery
            WHERE time IN ('202501')
        )
        ORDER BY time
    """

    sql "drop table if exists test_having_subquery"
}