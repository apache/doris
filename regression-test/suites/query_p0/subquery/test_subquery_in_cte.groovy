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

// Regression test for duplicate RelationId bug in simple CASE WHEN with subqueries.
// The bug caused "groupExpression already exists in memo" error because the simple
// case value (a subquery) was duplicated into multiple EqualTo nodes at parse time.

suite("test_subquery_in_cte") {

    multi_sql """
        DROP TABLE IF EXISTS test_subquery_in_cte_users;
        DROP TABLE IF EXISTS test_subquery_in_cte_orders;
        CREATE TABLE test_subquery_in_cte_users (
        user_id INT,
        region VARCHAR(16)
        )
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num"="1");
        CREATE TABLE test_subquery_in_cte_orders (
        order_id INT,
        user_id INT,
        qty INT
        )
        DUPLICATE KEY(order_id, user_id)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES("replication_num"="1");
        INSERT INTO test_subquery_in_cte_users VALUES (1, 'east'), (2, 'west');
        INSERT INTO test_subquery_in_cte_orders VALUES (10, 1, 2), (11, 2, 1);
    """
    qt_select_exists """SELECT
                    u.region,
                    COUNT(*) AS user_cnt
                    FROM test_subquery_in_cte_users u
                    WHERE EXISTS (
                    WITH picked AS (
                        SELECT
                        o.user_id AS uid
                        FROM test_subquery_in_cte_orders o
                        WHERE o.qty >= 2
                    )
                    SELECT 1
                    FROM picked p
                    WHERE p.uid = u.user_id
                    )
                    GROUP BY u.region
                    ORDER BY 1, 2;
    """

    qt_select_scalar """SELECT
                    u.region,
                    COUNT(*) AS user_cnt
                    FROM test_subquery_in_cte_users u
                    WHERE user_id = (
                    WITH picked AS (
                        SELECT
                        o.user_id AS uid
                        FROM test_subquery_in_cte_orders o
                        WHERE o.qty >= 2
                    )
                    SELECT uid
                    FROM picked p
                    )
                    GROUP BY u.region
                    ORDER BY 1, 2;
    """

    qt_select_in """SELECT
                    u.region,
                    COUNT(*) AS user_cnt
                    FROM test_subquery_in_cte_users u
                    WHERE user_id in (
                    WITH picked AS (
                        SELECT
                        o.user_id AS uid
                        FROM test_subquery_in_cte_orders o
                        WHERE o.qty >= 2
                    )
                    SELECT uid
                    FROM picked p
                    )
                    GROUP BY u.region
                    ORDER BY 1, 2;
    """
}
