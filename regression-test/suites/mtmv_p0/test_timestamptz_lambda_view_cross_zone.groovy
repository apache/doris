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

import org.junit.Assert;

/**
 * Expanding a stored view into a query in a different session zone wraps its zone-sensitive expressions in
 * guess-guards. A lambda is a structural root: array_map and the other higher-order functions require their
 * lambda argument to stay a Lambda (ArrayMap asserts it when rebuilding its children), so the guard must be
 * placed on the lambda body and on the enclosing call instead of on the lambda itself. Otherwise reading the
 * view across zones fails to analyze with "array_map's first child must be a lambda".
 */
suite("test_timestamptz_lambda_view_cross_zone","mtmv") {
    def dbName = "timestamptz_lambda_view_cross_zone"
    def tableName = "timestamptz_lambda_view_cross_zone_table"
    def viewName = "timestamptz_lambda_view_cross_zone_view"
    def arrayViewName = "timestamptz_lambda_view_cross_zone_array_view"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def viewTz = '+08:00'
    def queryTz = '-05:00'

    sql "SET time_zone = '${viewTz}'"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            arr ARRAY<VARCHAR(32)>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName} VALUES (1, ['2024-01-01 02:30:00','2024-01-02 05:00:00'])"
    sql "sync"

    // The cast inside the lambda body is zone sensitive, so expanding the view in another zone guards it.
    sql "SET time_zone = '${viewTz}'"
    sql "DROP VIEW IF EXISTS ${viewName}"
    sql """
        CREATE VIEW ${viewName} AS
        SELECT id, element_at(array_map(x -> CAST(x AS TIMESTAMPTZ(6)), arr), 1) AS ts1
        FROM ${tableName}
    """
    sql "DROP VIEW IF EXISTS ${arrayViewName}"
    sql """
        CREATE VIEW ${arrayViewName} AS
        SELECT id, array_map(x -> CAST(x AS TIMESTAMPTZ(6)), arr)
        FROM ${tableName}
    """

    // Same zone: no guard, the instant keeps its +08:00 rendering.
    def sameZone = sql "SELECT id, CAST(ts1 AS STRING) FROM ${viewName} ORDER BY 1"
    Assert.assertEquals(1, sameZone.size())
    Assert.assertTrue("expected 2024-01-01 02:30:00.000000+08:00, got " + sameZone[0][1],
            sameZone[0][1].toString().contains("2024-01-01 02:30:00"))

    // Cross zone: the view must still analyze and read; the instant itself is preserved and only the
    // rendering offset follows the query session.
    sql "SET time_zone = '${queryTz}'"
    def crossZone = sql "SELECT id, CAST(ts1 AS STRING) FROM ${viewName} ORDER BY 1"
    Assert.assertEquals(1, crossZone.size())
    Assert.assertTrue("expected the instant 2024-01-01 02:30:00, got " + crossZone[0][1],
            crossZone[0][1].toString().contains("2024-01-01 02:30:00"))

    // The array-valued projection keeps array_map's lambda child intact as well.
    def crossArray = sql "SELECT id, CAST(array_map(x -> CAST(x AS TIMESTAMPTZ(6)), arr) AS STRING) FROM ${tableName} ORDER BY 1"
    def crossArrayView = sql "SELECT * FROM ${arrayViewName} ORDER BY 1"
    Assert.assertEquals(1, crossArrayView.size())
    Assert.assertEquals("the cross-zone view must return the same value as the direct query",
            crossArray[0][1].toString(), crossArrayView[0][1].toString())
    Assert.assertTrue("expected the instant 2024-01-01 02:30:00, got " + crossArrayView[0][1],
            crossArrayView[0][1].toString().contains("2024-01-01 02:30:00"))

    // Reading the array view back in its own zone must agree as well.
    sql "SET time_zone = '${viewTz}'"
    def sameArrayView = sql "SELECT * FROM ${arrayViewName} ORDER BY 1"
    Assert.assertEquals(1, sameArrayView.size())
    Assert.assertTrue("expected the instant 2024-01-01 02:30:00, got " + sameArrayView[0][1],
            sameArrayView[0][1].toString().contains("2024-01-01 02:30:00"))
}
