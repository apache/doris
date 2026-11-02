
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

suite("or_to_in_complex_type") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists or_to_in_array_null"

    sql """
        CREATE TABLE or_to_in_array_null (
            id INT,
            a ARRAY<INT> NULL,
            k INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO or_to_in_array_null VALUES
            (1, NULL, 0),
            (2, [1], 0),
            (3, [3], 0)
    """

    // OrToIn in extract mode relaxes the independence check for "slot = literal" candidates so
    // that a top-level "slot IN (...)" can still be inferred when the slot is shared with a
    // sibling conjunct. That relaxation is only sound while "=" and IN agree on NULL.
    //
    // For ARRAY, IN is executed by FunctionCollectionIn, which unwraps the left nullable column
    // without merging its null map: for a = NULL it returns a non-null FALSE where "a = [1]"
    // returns NULL. So "a IN ([1], [2]) AND (<original OR>)" is no longer equivalent to the
    // original OR, and a NULL-sensitive parent changes the result.
    //
    // Correct answer: only id=1, whose OR evaluates to NULL.
    // With the array candidate admitted, the added IN yields FALSE for id=1, so
    // "FALSE AND NULL" makes IS NULL false and the row disappears.
    qt_array_shared_slot_is_null """
        SELECT id FROM or_to_in_array_null
        WHERE ((a = [1] AND size(a) > k) OR a = [2]) IS NULL
        ORDER BY id
    """

    // The same relaxation must still fire for scalar types, otherwise the optimization loses
    // its value for the STRING/INT predicates it was written for.
    // id=4 is the NULL row: the inferred "s IN ('x', 'y')" keeps NULL, so the OR stays NULL.
    sql "drop table if exists or_to_in_scalar_null"
    sql """
        CREATE TABLE or_to_in_scalar_null (
            id INT,
            s VARCHAR(16) NULL,
            k INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO or_to_in_scalar_null VALUES
            (1, 'x', 0),
            (2, 'other', 0),
            (3, 'z', 0),
            (4, NULL, 0)
    """
    qt_scalar_shared_slot_is_null """
        SELECT id FROM or_to_in_scalar_null
        WHERE ((s = 'x' AND length(s) > k) OR s = 'y') IS NULL
        ORDER BY id
    """

    // The inferred IN is only appended, never substituted, so the plain WHERE form must return
    // exactly the rows the OR selects.
    qt_scalar_shared_slot_where """
        SELECT id FROM or_to_in_scalar_null
        WHERE (s = 'x' AND length(s) > k) OR s = 'y'
        ORDER BY id
    """

    qt_array_shared_slot_where """
        SELECT id FROM or_to_in_array_null
        WHERE (a = [1] AND size(a) > k) OR a = [2]
        ORDER BY id
    """
}
