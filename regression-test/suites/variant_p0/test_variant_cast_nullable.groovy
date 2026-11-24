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

suite("test_variant_cast_nullable", "p0") {
    // This regression covers legacy Variant conversion results; V2 has different conversion semantics.
    if (getFeConfig("enable_variant_v2").toBoolean()) {
        return
    }
    sql "DROP TABLE IF EXISTS variant_cast_nullable"
    sql """
        CREATE TABLE variant_cast_nullable (
            id INT NOT NULL,
            v VARIANT NOT NULL,
            nullable_v VARIANT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_cast_nullable VALUES
        (1, 'null', NULL),
        (2, '42', '42'),
        (3, '{"a":1}', '{"a":1}'),
        (4, '"invalid"', '"invalid"'),
        (5, '[1,2]', '[1,2]')
    """

    qt_casts """
        SELECT id, CAST(v AS STRING), CAST(v AS INT), CAST(nullable_v AS INT)
        FROM variant_cast_nullable ORDER BY id
    """
    qt_null_predicates """
        SELECT id, CAST(v AS STRING) IS NULL, CAST(v AS STRING) <=> NULL,
               CAST(v AS INT) IS NULL, CAST(v AS INT) <=> NULL,
               CAST(v AS VARIANT) IS NULL
        FROM variant_cast_nullable ORDER BY id
    """
    qt_is_null_filter """
        SELECT id FROM variant_cast_nullable WHERE CAST(v AS STRING) IS NULL ORDER BY id
    """
    qt_is_not_null_filter """
        SELECT id FROM variant_cast_nullable WHERE CAST(v AS INT) IS NOT NULL ORDER BY id
    """
    qt_nullable_input_filter """
        SELECT id FROM variant_cast_nullable WHERE CAST(nullable_v AS INT) IS NULL ORDER BY id
    """
    qt_null_safe_filter """
        SELECT id FROM variant_cast_nullable WHERE CAST(v AS INT) <=> NULL ORDER BY id
    """
    qt_fallbacks """
        SELECT id, COALESCE(CAST(v AS STRING), 'fallback'), NVL(CAST(v AS STRING), 'fallback'),
               COALESCE(CAST(v AS INT), 99), NVL(CAST(v AS INT), 99),
               COALESCE(CAST(nullable_v AS INT), 99), NVL(CAST(nullable_v AS INT), 99)
        FROM variant_cast_nullable ORDER BY id
    """
    qt_alias_null_predicate """
        WITH casts AS (SELECT id, CAST(v AS INT) AS value FROM variant_cast_nullable)
        SELECT id, value, COALESCE(value, 99) FROM casts WHERE value IS NULL ORDER BY id
    """

    // Keep an all-Variant-null root to exercise the legacy all-default-values cast path.
    sql "DROP TABLE IF EXISTS variant_cast_nullable_nulls"
    sql """
        CREATE TABLE variant_cast_nullable_nulls (
            id INT NOT NULL,
            v VARIANT NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO variant_cast_nullable_nulls VALUES (1, 'null'), (2, 'null')"
    qt_all_null_filter """
        SELECT id FROM variant_cast_nullable_nulls WHERE CAST(v AS STRING) IS NULL ORDER BY id
    """
    qt_all_not_null_filter """
        SELECT id FROM variant_cast_nullable_nulls WHERE CAST(v AS STRING) IS NOT NULL ORDER BY id
    """
    qt_all_null_roots """
        SELECT id, CAST(v AS STRING), CAST(v AS STRING) IS NULL, CAST(v AS STRING) <=> NULL,
               COALESCE(CAST(v AS STRING), 'fallback'), NVL(CAST(v AS STRING), 'fallback'),
               COALESCE(CAST(v AS INT), 99), NVL(CAST(v AS INT), 99)
        FROM variant_cast_nullable_nulls ORDER BY id
    """
}
