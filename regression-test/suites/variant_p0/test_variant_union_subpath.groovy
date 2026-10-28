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

suite("test_variant_union_subpath", "p0") {
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS variant_union_subpath"
    sql """
        CREATE TABLE variant_union_subpath (
            id INT, a VARIANT, b VARIANT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_union_subpath VALUES
        (2, CAST(json_parse('{"x":11,"z":99}') AS VARIANT),
            CAST(json_parse('{"x":22,"z":88}') AS VARIANT))
    """

    // Repeated CTE consumers must keep each UNION output's sub-path tied to its original column.
    order_qt_repeated_variant_output """
        WITH u AS (
            SELECT a, b, a AS c FROM variant_union_subpath WHERE id = 2
            UNION ALL
            SELECT a, b, a AS c FROM variant_union_subpath WHERE id = 2
        )
        SELECT u1.a['x'], u1.b['x'], u1.c['x']
        FROM u u1 JOIN u u2 ON u1.a['x'] = u2.a['x']
    """

    // A whole-column read has no sub-path entry, even when another constant column has one.
    order_qt_constant_whole_column """
        WITH u AS (
            SELECT CAST(json_parse('{"x":11}') AS VARIANT) a,
                   CAST(json_parse('{"x":22}') AS VARIANT) b
            UNION ALL
            SELECT CAST(json_parse('{"x":33}') AS VARIANT),
                   CAST(json_parse('{"x":44}') AS VARIANT)
        )
        SELECT u1.a, u1.b['x']
        FROM u u1 JOIN u u2 ON u1.b['x'] = u2.b['x']
    """

    // Different requested paths and a constant branch exercise both UNION input representations.
    order_qt_mixed_inputs_and_paths """
        WITH u AS (
            SELECT a, b, a AS c FROM variant_union_subpath
            UNION ALL
            SELECT CAST(json_parse('{"x":33,"z":77}') AS VARIANT),
                   CAST(json_parse('{"x":44,"z":66}') AS VARIANT),
                   CAST(json_parse('{"x":33,"z":77}') AS VARIANT)
        )
        SELECT u1.a['x'], u1.a['z'], u1.b['z'], u1.c['x']
        FROM u u1 JOIN u u2 ON u1.a['x'] = u2.a['x']
    """

    order_qt_constant_multiple_subpaths """
        WITH u AS (
            SELECT CAST(json_parse('{"x":11,"z":99}') AS VARIANT) a,
                   CAST(json_parse('{"x":22,"z":88}') AS VARIANT) b
            UNION ALL
            SELECT CAST(json_parse('{"x":33,"z":77}') AS VARIANT),
                   CAST(json_parse('{"x":44,"z":66}') AS VARIANT)
        )
        SELECT u1.a['x'], u1.a['z'], u1.b['x'], u1.b['z']
        FROM u u1 JOIN u u2 ON u1.a['x'] = u2.a['x']
    """
}
