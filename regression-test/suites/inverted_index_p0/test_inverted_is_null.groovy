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

suite("test_inverted_is_null", "p0") {
    def tableName = "tbl_inverted_is_null"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            dt DATE NULL,
            str_col STRING NULL,
            val INT NULL,
            INDEX idx_dt (dt) USING INVERTED,
            INDEX idx_str (str_col) USING INVERTED,
            INDEX idx_val (val) USING INVERTED
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, NULL, 'foo', 1),
        (2, NULL, 'bar', -1),
        (3, '2024-01-01', 'baz', 5),
        (4, NULL, 'qux', 10)
    """

    sql "SET enable_common_expr_pushdown=true"
    sql "SET inverted_index_skip_threshold=0"

    def nullBranchQuery = """
        SELECT COUNT(*)
        FROM ${tableName}
        WHERE (str_col LIKE CONCAT('%', 'no-hit', '%'))
           OR (dt IS NULL) AND NOT val BETWEEN -9223372036854775808 AND 0
    """

    def negatedNotNullQuery = """
        SELECT COUNT(*)
        FROM ${tableName}
        WHERE NOT (dt IS NOT NULL)
    """

    sql "SET enable_inverted_index_query=true"
    def resultWithIndex = sql(nullBranchQuery)
    def resultWithIndexNegatedNotNull = sql(negatedNotNullQuery)
    assertEquals(2, resultWithIndex[0][0])      // previously returned 0 when dt IS NULL relied on inverted index
    assertEquals(3, resultWithIndexNegatedNotNull[0][0]) // previously returned 0 when NOT (dt IS NOT NULL) was evaluated via inverted index

    sql "SET enable_inverted_index_query=false"
    def resultWithoutIndex = sql(nullBranchQuery)
    def resultWithoutIndexNegatedNotNull = sql(negatedNotNullQuery)
    assertEquals(2, resultWithoutIndex[0][0])
    assertEquals(3, resultWithoutIndexNegatedNotNull[0][0])
}
