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

// Access paths are collected with a lower-cased column name, while a whole-column access path
// keeps the name as the catalog stores it. When a column name is not all lower case, the two
// spellings must still be recognized as the same column, otherwise the sub-field path of the
// predicate is added next to the whole-column path and the BE reads that field only, leaving the
// sibling fields of the projected column empty.

suite("upper_case_nested_column_pruning") {
    sql """ SET enable_prune_nested_column = true """
    sql """ DROP TABLE IF EXISTS ucnp_tbl """
    sql """
        CREATE TABLE ucnp_tbl (
            id  INT,
            S   STRUCT<City: STRING, Zip: INT> NULL,
            M   MAP<STRING, INT> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    sql """
        INSERT INTO ucnp_tbl VALUES
            (1, named_struct('city', 'beijing', 'zip', 10001), {'a': 1, 'b': 2})
    """

    // The whole struct is projected and the predicate reads one field of it.
    def structRows = sql """ SELECT S FROM ucnp_tbl WHERE struct_element(S, 'City') = 'beijing' """
    assertEquals(1, structRows.size())
    assertTrue(structRows[0][0].toString().contains("beijing"),
            "projected struct lost City: ${structRows[0][0]}")
    assertTrue(structRows[0][0].toString().contains("10001"),
            "projected struct lost the sibling field Zip: ${structRows[0][0]}")

    // Same shape on a map column: every key and value must survive a predicate on the values.
    def mapRows = sql """ SELECT M FROM ucnp_tbl WHERE array_contains(map_values(M), 1) """
    assertEquals(1, mapRows.size())
    def mapValue = mapRows[0][0].toString()
    for (String entry : ["a", "b", "1", "2"]) {
        assertTrue(mapValue.contains(entry),
                "projected map lost ${entry}: ${mapValue}")
    }
}
