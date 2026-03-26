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

suite("test_search_mow_support", "p0") {
    def tableName = "search_mow_support_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            k1 BIGINT,
            title VARCHAR(256),
            desc_text TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    // initial rows
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'Rainbowman', 'first version'),
        (2, 'Other Title', 'unrelated row'),
        (3, 'Rainbowman Story', 'should not match exact search')
    """

    // update existing key to exercise delete bitmap handling
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'Rainbowman', 'updated version'),
        (4, 'rainbowman lowercase', 'case variants')
    """

    sql "SET enable_common_expr_pushdown = true"
    sql "SET enable_common_expr_pushdown_for_inverted_index = true"

    def exactCount = sql """
        SELECT /*+SET_VAR(enable_inverted_index_query=true) */ count(*)
        FROM ${tableName}
        WHERE search('title:ALL("Rainbowman")')
    """
    assert exactCount[0][0] == 3

    def exactRows = sql """
        SELECT /*+SET_VAR(enable_inverted_index_query=true) */ k1, title
        FROM ${tableName}
        WHERE search('title:ALL("Rainbowman")')
        ORDER BY k1
    """
    assert exactRows.size() == 3
    assert exactRows[0][0] == 1
    assert exactRows[0][1] == 'Rainbowman'

    def anyCount = sql """
        SELECT /*+SET_VAR(enable_inverted_index_query=true) */ count(*)
        FROM ${tableName}
        WHERE search('title:ANY("rainbowman")')
    """
    assert anyCount[0][0] == 3
}
