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

suite("test_correlated_outer_cte_alias_shadow") {
    sql """
        DROP TABLE IF EXISTS test_correlated_outer_cte_alias_shadow_outer;
        DROP TABLE IF EXISTS test_correlated_outer_cte_alias_shadow_inner;
    """

    sql """
        CREATE TABLE test_correlated_outer_cte_alias_shadow_outer (
            id INT,
            grp INT,
            dt DATE,
            ts6 DATETIMEV2(6) NOT NULL,
            ts3_n DATETIMEV2(3) NULL,
            i_nn INT NOT NULL,
            i_n INT NULL,
            j_nn INT NOT NULL
        )
        DUPLICATE KEY(id, grp, dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        CREATE TABLE test_correlated_outer_cte_alias_shadow_inner (
            id INT,
            grp INT,
            dt DATE,
            ts6 DATETIMEV2(6) NOT NULL,
            ts3_n DATETIMEV2(3) NULL,
            i_nn INT NOT NULL,
            i_n INT NULL,
            j_nn INT NOT NULL,
            s VARCHAR(32) NULL
        )
        DUPLICATE KEY(id, grp, dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_correlated_outer_cte_alias_shadow_outer VALUES
        (1, 10,
         CURRENT_DATE(),
         CAST(CONCAT(CURRENT_DATE(), ' 12:00:00.000000') AS DATETIMEV2(6)),
         CAST(CONCAT(CURRENT_DATE(), ' 12:00:00.000') AS DATETIMEV2(3)),
         5, NULL, 5);
    """

    sql """
        INSERT INTO test_correlated_outer_cte_alias_shadow_inner VALUES
        (1, 10,
         CURRENT_DATE(),
         CAST(CONCAT(CURRENT_DATE(), ' 12:00:00.000000') AS DATETIMEV2(6)),
         CAST(CONCAT(CURRENT_DATE(), ' 12:00:00.000') AS DATETIMEV2(3)),
         5, NULL, 5, 'x');
    """

    order_qt_correlated_outer_cte_alias_shadow """
        WITH seed AS (
            SELECT id, grp, dt, ts6, ts3_n, i_nn, i_n, j_nn
            FROM test_correlated_outer_cte_alias_shadow_outer
            UNION ALL
            SELECT id, grp, dt, ts6, ts3_n, i_nn, i_n, j_nn
            FROM test_correlated_outer_cte_alias_shadow_outer
            WHERE 1 = 0
        )
        SELECT id
        FROM seed s
        WHERE s.i_nn IN (
            SELECT CASE WHEN b.i_n IS NULL THEN b.j_nn ELSE b.i_nn END
            FROM test_correlated_outer_cte_alias_shadow_inner b
            WHERE b.grp = s.grp
              AND (DATE(b.ts6) = s.dt OR b.ts3_n <=> s.ts3_n)
        )
        ORDER BY id;
    """
}