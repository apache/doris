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

suite("test_file_type") {
    def publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT," +
            "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>"
    def fileCtes = """
        WITH test_file_type_values AS (
            SELECT id, CAST(j AS FILE) AS f FROM (
                SELECT 1 AS id, NAMED_STRUCT('uri', 'S3://Bucket/a%2fb?versionId=v1', 'size', CAST(0 AS BIGINT),
                    'content_type', 'Image/PNG', 'checksum', 'CRC32:00000000', 'offset', CAST(NULL AS BIGINT), 'inline', X'00FF80') AS j
                UNION ALL SELECT 2, NULL
                UNION ALL SELECT 3, NAMED_STRUCT('uri', 'urn:example:three', 'size', CAST(8 AS BIGINT),
                    'content_type', NULL, 'checksum', NULL, 'offset', CAST(2 AS BIGINT), 'inline', X'')
            ) scalar_rows
        ), test_file_type_nested AS (
            SELECT id, ARRAY(f, NULL) AS files,
                   NAMED_STRUCT('f', f) AS holder, MAP('source', f) AS lookup
            FROM test_file_type_values
        ), test_file_type_inline_values AS (
            SELECT id, CAST(j AS FILE) AS f FROM (
                SELECT 1 AS id, NAMED_STRUCT('uri', CAST(NULL AS STRING), 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', X'00FF') AS j
                UNION ALL SELECT 2, NAMED_STRUCT('uri', NULL, 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', X'')
                UNION ALL SELECT 3, NULL
                UNION ALL SELECT 4, NAMED_STRUCT('uri', 'urn:reference', 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', NULL)
            ) scalar_inline_rows
        )
    """
    def fileQuery = { String statement -> "${fileCtes} ${statement}" }
    qt_reordered_struct fileQuery("""
        SELECT CAST(NAMED_STRUCT('SIZE', CAST(CAST(8 AS SMALLINT) AS BIGINT),
               'URI', 'urn:example:three', 'OFFSET', CAST(CAST(2 AS INT) AS BIGINT),
               'content_type', NULL, 'checksum', NULL, 'inline', X'') AS FILE)
    """)
    qt_values fileQuery("SELECT id, f, CAST(TO_JSON(f) AS STRING), TO_JSON(f) FROM test_file_type_values ORDER BY id")
    qt_to_json_nested fileQuery("""
        SELECT id, TO_JSON(files), TO_JSON(holder), TO_JSON(lookup)
        FROM test_file_type_nested ORDER BY id
    """)
    qt_to_json_inline fileQuery("SELECT id, TO_JSON(f) FROM test_file_type_inline_values ORDER BY id")
    qt_fields fileQuery("""
        SELECT id, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'), ELEMENT_AT(f, 'size'),
               ELEMENT_AT(f, 'content_type'), ELEMENT_AT(f, 'checksum'), ELEMENT_AT(f, 'UrI'),
               FROM_BINARY(FL_GET_BINARY(f)), FROM_BINARY(FL_GET_BINARY(f)),
               FL_GET_BINARY(f) IS NULL,
               f IS NULL, f IS NOT NULL
        FROM test_file_type_values ORDER BY id
    """)
    qt_count fileQuery("SELECT COUNT(f) FROM test_file_type_values")
    qt_ordinary_constant_element """
        SELECT ELEMENT_AT(ARRAY(10, 20), 2), ELEMENT_AT(MAP('asset', 30), 'asset')
    """
    def constantFoldMode = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    def largeConstantFile = """CAST(NAMED_STRUCT(
            'uri', 'urn:large-constant', 'offset', NULL, 'size', NULL,
            'content_type', NULL, 'checksum', NULL,
            'inline', CAST(REPEAT('x', 1048576) AS VARBINARY)) AS FILE)"""
    try {
        for (def foldMode in [false, true]) {
            sql "SET enable_fold_constant_by_be = ${foldMode}"
            quickTest("to_json_values_${foldMode}", fileQuery("""
                SELECT id, f, CAST(TO_JSON(f) AS STRING), TO_JSON(f)
                FROM test_file_type_values ORDER BY id
            """))
            quickTest("to_json_nested_${foldMode}", fileQuery("""
                SELECT id, TO_JSON(files), TO_JSON(holder), TO_JSON(lookup)
                FROM test_file_type_nested ORDER BY id
            """))
            quickTest("to_json_inline_${foldMode}", fileQuery("""
                SELECT id, TO_JSON(f) FROM test_file_type_inline_values ORDER BY id
            """))
            quickTest("constant_container_metadata_${foldMode}", """
                SELECT COUNT(*), MIN(array_uri), MAX(map_uri), MIN(struct_uri)
                FROM (
                    SELECT ELEMENT_AT(ELEMENT_AT(ARRAY(${largeConstantFile}), 1), 'uri') AS array_uri,
                           ELEMENT_AT(ELEMENT_AT(MAP('asset', ${largeConstantFile}), 'asset'), 'uri') AS map_uri,
                           ELEMENT_AT(ELEMENT_AT(NAMED_STRUCT('asset', ${largeConstantFile}), 'asset'), 'uri') AS struct_uri
                    FROM NUMBERS("number" = "4096")
                ) projected
            """)
        }
    } finally {
        sql "SET enable_fold_constant_by_be = ${constantFoldMode}"
    }
    // Multiple WHEN arms retain CASE rather than being rewritten to IF.
    def originalShortCircuit = sql("SELECT @@short_circuit_evaluation")[0][0]
    try {
        for (def shortCircuit in [false, true]) {
            sql "SET short_circuit_evaluation = ${shortCircuit}"
            def selected = sql """
                SELECT number, ELEMENT_AT(chosen, 'uri'),
                       FROM_BINARY(FL_GET_BINARY(chosen)), CAST(chosen IS NULL AS INT)
                FROM (
                    SELECT number,
                           CASE WHEN number % 3 = 0 THEN f
                                WHEN number % 3 = 1 THEN CAST(NULL AS FILE)
                                ELSE g END AS chosen
                    FROM (
                        SELECT number,
                               CAST(NAMED_STRUCT('uri', CONCAT('urn:left:', number),
                                   'offset', NULL, 'size', NULL, 'content_type', NULL,
                                   'checksum', NULL, 'inline',
                                   CAST(CONCAT('L', number) AS VARBINARY)) AS FILE) AS f,
                               CAST(NAMED_STRUCT('uri', CONCAT('urn:right:', number),
                                   'offset', NULL, 'size', NULL, 'content_type', NULL,
                                   'checksum', NULL, 'inline',
                                   CAST(CONCAT('R', number) AS VARBINARY)) AS FILE) AS g
                        FROM NUMBERS("number" = "6")
                    ) inputs
                ) selections ORDER BY number
            """
            assertEquals([["0", "urn:left:0", "4C30", "0"], ["1", null, null, "1"],
                          ["2", "urn:right:2", "5232", "0"], ["3", "urn:left:3", "4C33", "0"],
                          ["4", null, null, "1"], ["5", "urn:right:5", "5235", "0"]],
                    selected.collect { row -> row.collect { value -> value == null ? null : value.toString() } })
        }
    } finally {
        sql "SET short_circuit_evaluation = ${originalShortCircuit}"
    }
    // Both functions compare elements, including FILE nested below a container.
    for (def function in ["ARRAY_REVERSE_SORT", "ARRAY_COMPACT"]) {
        for (def values in ["ARRAY(f, f)", "ARRAY(ARRAY(f), ARRAY(f))",
                            "ARRAY(NAMED_STRUCT('f', f), NAMED_STRUCT('f', f))",
                            "ARRAY(MAP('f', f), MAP('f', f))"]) {
            test {
                sql """SELECT ${function}(${values}) FROM (
                    SELECT CAST(NAMED_STRUCT('uri', CONCAT('urn:key:', number),
                        'offset', NULL, 'size', NULL, 'content_type', NULL,
                        'checksum', NULL, 'inline', NULL) AS FILE) AS f
                    FROM NUMBERS("number" = "2")
                ) inputs"""
                exception "FILE"
            }
        }
    }
    for (def key in ["f", "ARRAY(f)", "NAMED_STRUCT('f', f)", "MAP('asset', f)"]) {
        test {
            sql """EXPLAIN SELECT MAP(${key}, number) FROM (
                SELECT number, CAST(NAMED_STRUCT('uri', CONCAT('urn:map-key:', number),
                    'offset', NULL, 'size', NULL, 'content_type', NULL,
                    'checksum', NULL, 'inline', NULL) AS FILE) AS f
                FROM NUMBERS("number" = "2")
            ) inputs"""
            exception "MAP key does not support FILE"
        }
    }
    def inlineFallback = """CAST(NAMED_STRUCT('uri', NULL, 'offset', NULL, 'size', NULL,
            'content_type', NULL, 'checksum', NULL, 'inline', X'00FF80') AS FILE)"""
    qt_conditional_values fileQuery("""
        SELECT id, COALESCE(f, ${inlineFallback}), IF(id = 2, ${inlineFallback}, f),
               IFNULL(f, ${inlineFallback}), CASE WHEN id = 2 THEN ${inlineFallback} ELSE f END,
               FROM_BINARY(FL_GET_BINARY(COALESCE(f, ${inlineFallback})))
        FROM test_file_type_values ORDER BY id
    """)
    qt_null_only_propagation fileQuery("""
        SELECT COALESCE(CAST(NULL AS FILE), NULL), IF(TRUE, CAST(NULL AS FILE), NULL),
               IFNULL(NULL, CAST(NULL AS FILE)), ARRAY(CAST(NULL AS FILE), NULL),
               NAMED_STRUCT('f', CAST(NULL AS FILE)), MAP('source', CAST(NULL AS FILE)),
               COALESCE(CAST(NULL AS ARRAY<FILE>), NULL),
               IF(FALSE, CAST(NULL AS STRUCT<f:FILE>), NULL),
               IFNULL(CAST(NULL AS MAP<STRING,FILE>), NULL)
    """)
    qt_ordinary_functions fileQuery("""
        SELECT ARRAY_MAP(x -> x + 1, ARRAY(2, 1)), ARRAY_SORTBY(ARRAY('b', 'a'), ARRAY(2, 1)),
               COALESCE(NULL, 9), IF(TRUE, 'x', 'y'), NAMED_STRUCT('k', 5), MAP('k', 5)
    """)
    qt_count_null_input fileQuery("SELECT COUNT(f), COUNT(*) FROM test_file_type_values WHERE f IS NULL")
    qt_count_window fileQuery("""
        SELECT id, COUNT(f) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        FROM test_file_type_values ORDER BY id
    """)
    qt_count_empty_containers fileQuery("""
        SELECT COUNT(files), COUNT(lookup)
        FROM (SELECT CAST(ARRAY() AS ARRAY<FILE>) files, CAST(MAP() AS MAP<STRING,FILE>) lookup) t
    """)
    qt_count_fields fileQuery("""
        SELECT COUNT(ELEMENT_AT(f, 'uri')), COUNT(ELEMENT_AT(f, 'size')),
               COUNT(FL_GET_BINARY(f)) FROM test_file_type_values
    """)
    test {
        sql fileQuery("SELECT ANY_VALUE(f) FROM test_file_type_values")
        exception "Agg Function any_value(FILE) is not implemented"
    }
    for (def aggregate in ["MIN_BY(f, id)", "MAX_BY(f, id)"]) {
        test {
            sql fileQuery("SELECT ${aggregate} FROM test_file_type_values")
            exception "Illegal type File of argument of aggregate function min/max_by"
        }
    }
    for (def aggregate in ["ARRAY_AGG(f)", "COLLECT_LIST(f)", "MAP_AGG(id, f)",
                          "ANY_VALUE(ARRAY(f))", "ARRAY_AGG(NAMED_STRUCT('asset', f))"]) {
        sql fileQuery("SELECT ${aggregate} FROM test_file_type_values")
    }
    test {
        sql fileQuery("""
            SELECT (SELECT b.f FROM test_file_type_values b WHERE b.id = a.id)
            FROM test_file_type_values a
        """)
        exception "Agg Function any_value(FILE) is not implemented"
    }
    def originalAggState = sql("SELECT @@enable_agg_state")[0][0]
    sql "SET enable_agg_state = true"
    try {
        test {
            sql "SELECT CAST(NULL AS AGG_STATE<any_value(FILE)>)"
            exception "FILE"
        }
    } finally {
        sql "SET enable_agg_state = ${originalAggState}"
    }
    qt_scalar_group fileQuery("SELECT ELEMENT_AT(f, 'size'), COUNT(*) FROM test_file_type_values GROUP BY ELEMENT_AT(f, 'size') ORDER BY 1")
    qt_scalar_join fileQuery("""
        SELECT a.id, b.f FROM test_file_type_values a JOIN test_file_type_values b
        ON a.id = b.id ORDER BY a.id
    """)
    qt_window_payload fileQuery("SELECT id, f, ROW_NUMBER() OVER (ORDER BY id) FROM test_file_type_values ORDER BY id")
    qt_window_scalar_fields fileQuery("""
        SELECT id, LAG(ELEMENT_AT(f, 'uri')) OVER (ORDER BY id),
               LEAD(ELEMENT_AT(f, 'size')) OVER (ORDER BY id),
               FIRST_VALUE(ELEMENT_AT(f, 'uri')) OVER (ORDER BY id),
               LAST_VALUE(ELEMENT_AT(f, 'size')) OVER (ORDER BY id)
        FROM test_file_type_values ORDER BY id
    """)
    for (def expression in [
        "FIRST_VALUE(f) OVER (ORDER BY id)",
        "LAST_VALUE(f) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "NTH_VALUE(f, 1) OVER (ORDER BY id)", "NULLIF(f, f)"
    ]) {
        sql fileQuery("SELECT ${expression} FROM test_file_type_values")
    }
    for (def expression in [
        "LAG(f) OVER (ORDER BY id)", "LEAD(f) OVER (ORDER BY id)"
    ]) {
        test {
            sql fileQuery("SELECT ${expression} FROM test_file_type_values")
            exception "FILE"
        }
    }
    sql "DROP FUNCTION IF EXISTS test_file_alias_size(FILE)"
    sql """CREATE ALIAS FUNCTION test_file_alias_size(FILE)
           WITH PARAMETER(f) AS ELEMENT_AT(f, 'size')"""
    for (def expression in ["test_file_alias_size(NULL)", "test_file_alias_size(f)",
                           "ARRAY_MAP(x -> f, ARRAY(id))"]) {
        sql fileQuery("SELECT ${expression} FROM test_file_type_values")
    }
    qt_union_all fileQuery("""
        SELECT id, f FROM test_file_type_values
        UNION ALL SELECT 4, CAST(NAMED_STRUCT('uri', 'urn:four', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)
        ORDER BY id
    """)
    qt_explode fileQuery("""
        SELECT id, uri, off, len, mime, digest, FROM_BINARY(data) FROM test_file_type_values
        LATERAL VIEW EXPLODE_FILE(f) e AS uri, off, len, mime, digest, data ORDER BY id
    """)
    qt_explode_outer fileQuery("""
        SELECT id, uri, off, len, mime, digest, FROM_BINARY(data) FROM test_file_type_values
        LATERAL VIEW EXPLODE_FILE_OUTER(f) e AS uri, off, len, mime, digest, data ORDER BY id
    """)

    qt_nested_getters fileQuery("""
        SELECT id, ELEMENT_AT(ELEMENT_AT(files, 1), 'uri'),
               ELEMENT_AT(ELEMENT_AT(holder, 'f'), 'uri'), ELEMENT_AT(ELEMENT_AT(lookup, 'source'), 'uri')
        FROM test_file_type_nested ORDER BY id
    """)
    qt_nested_payload fileQuery("SELECT * FROM test_file_type_nested ORDER BY id")
    qt_nested_union_all fileQuery("""
        SELECT id, files, holder, lookup FROM test_file_type_nested
        UNION ALL SELECT 4, CAST(NULL AS ARRAY<FILE>), CAST(NULL AS STRUCT<f:FILE>),
                         CAST(NULL AS MAP<STRING,FILE>)
        ORDER BY id
    """)
    qt_nested_nulls fileQuery("""
        SELECT id, ELEMENT_AT(files, 2) IS NULL, ELEMENT_AT(holder, 'f') IS NULL
        FROM test_file_type_nested ORDER BY id
    """)
    qt_nested_count fileQuery("SELECT COUNT(files), COUNT(holder), COUNT(lookup) FROM test_file_type_nested")
    qt_nested_propagation fileQuery("""
        SELECT id, REVERSE(files), ARRAY_SLICE(files, 1, 1), ARRAY_CONCAT(files, files),
               MAP_VALUES(lookup), MAP_KEYS(lookup), ARRAY_SIZE(files), MAP_SIZE(lookup),
               CARDINALITY(files), CARDINALITY(lookup), STRUCT(ELEMENT_AT(holder, 'f'))
        FROM test_file_type_nested ORDER BY id
    """)
    qt_conditional_containers fileQuery("""
        SELECT id, files, holder, lookup, files IS NULL, holder IS NULL, lookup IS NULL,
               ELEMENT_AT(files, 1) IS NULL, ELEMENT_AT(holder, 'f') IS NULL,
               ELEMENT_AT(lookup, 'source') IS NULL, ARRAY_SIZE(files), MAP_SIZE(lookup),
               REVERSE(files), ARRAY_SLICE(files, 1, 1), ARRAY_CONCAT(files, files),
               MAP_KEYS(lookup), MAP_VALUES(lookup)
        FROM (
            SELECT id, IF(id = 2, NULL, files) files,
                   COALESCE(IF(id = 2, NULL, holder), CAST(NULL AS STRUCT<f:FILE>)) holder,
                   IFNULL(IF(id = 2, NULL, lookup), CAST(NULL AS MAP<STRING,FILE>)) lookup
            FROM test_file_type_nested
        ) selected_containers ORDER BY id
    """)
    for (def expression in [
        "ARRAY_SORTBY(files, ARRAY(2, 1))", "ARRAY_MAP(x -> ELEMENT_AT(x, 'uri'), files)",
        "FIRST_VALUE(lookup) OVER (ORDER BY id)", "LAST_VALUE(files) OVER (ORDER BY id)",
        "NTH_VALUE(holder, 1) OVER (ORDER BY id)"
    ]) {
        sql fileQuery("SELECT ${expression} FROM test_file_type_nested")
    }
    for (def expression in [
        "ARRAY_SORT(files)", "ARRAY_DISTINCT(files)", "ARRAY_CONTAINS(files, ELEMENT_AT(files, 1))",
        "LAG(files) OVER (ORDER BY id)", "LEAD(holder) OVER (ORDER BY id)"
    ]) {
        test {
            sql fileQuery("SELECT ${expression} FROM test_file_type_nested")
            exception "FILE"
        }
    }
    sql fileQuery("""
        SELECT id, file_value FROM test_file_type_nested
        LATERAL VIEW EXPLODE(files) e AS file_value
    """)

    qt_uri_bytes_preserved """
        SELECT uri, ELEMENT_AT(CAST(NAMED_STRUCT('uri', uri, 'offset', NULL, 'size', NULL,
                       'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE), 'uri')
        FROM (SELECT '' AS uri UNION ALL SELECT 'relative/path'
              UNION ALL SELECT 's3://bucket/a b+中文%ZZ#fragment') t ORDER BY uri
    """
    sql "SET enable_strict_cast = true"
    for (def strict in [true, false]) {
        sql "SET enable_strict_cast = ${strict}"
        for (def castFunction in ["CAST", "TRY_CAST"]) {
            for (def expression in [
                "${castFunction}(f AS ${publicStruct})",
                "${castFunction}(f AS STRING)",
                "${castFunction}(ARRAY(f) AS STRING)",
                "${castFunction}(NAMED_STRUCT('f', f) AS STRING)",
                "${castFunction}(MAP('f', f) AS STRING)",
                "${castFunction}(CAST(NULL AS FILE) AS ${publicStruct})",
                "${castFunction}(ARRAY(f) AS ARRAY<${publicStruct}>)",
                "${castFunction}(NAMED_STRUCT('f', f) AS STRUCT<f:${publicStruct}>)",
                "${castFunction}(MAP('f', f) AS MAP<STRING,${publicStruct}>)",
                "${castFunction}(ARRAY(NAMED_STRUCT('f', f)) AS ARRAY<STRUCT<f:${publicStruct}>>)"
            ]) {
                test {
                    sql fileQuery("SELECT ${expression} FROM test_file_type_values")
                    exception "FILE"
                }
            }
            for (def externalType in ["JSON", "VARIANT"]) {
                for (def expression in [
                    "${castFunction}(f AS ${externalType})",
                    "${castFunction}(CAST(TO_JSON(f) AS ${externalType}) AS FILE)",
                    "${castFunction}(CAST(NULL AS FILE) AS ${externalType})",
                    "${castFunction}(CAST(NULL AS ${externalType}) AS FILE)",
                    "${castFunction}(ARRAY(f) AS ARRAY<${externalType}>)",
                    "${castFunction}(CAST(TO_JSON(ARRAY(f)) AS ARRAY<${externalType}>) AS ARRAY<FILE>)",
                    "${castFunction}(CAST(TO_JSON(ARRAY(f)) AS ${externalType}) AS ARRAY<FILE>)",
                    "${castFunction}(ARRAY(f) AS ${externalType})",
                    "${castFunction}(NAMED_STRUCT('f', f) AS ${externalType})",
                    "${castFunction}(CAST(TO_JSON(NAMED_STRUCT('f', f)) AS ${externalType}) AS STRUCT<f:FILE>)",
                    "${castFunction}(MAP('f', f) AS ${externalType})",
                    "${castFunction}(CAST(TO_JSON(MAP('f', f)) AS ${externalType}) AS MAP<STRING,FILE>)",
                    "${castFunction}(NAMED_STRUCT('f', f) AS STRUCT<f:${externalType}>)",
                    "${castFunction}(CAST(TO_JSON(NAMED_STRUCT('f', f)) AS STRUCT<f:${externalType}>) AS STRUCT<f:FILE>)",
                    "${castFunction}(MAP('f', f) AS MAP<STRING,${externalType}>)",
                    "${castFunction}(CAST(NULL AS MAP<STRING,${externalType}>) AS MAP<STRING,FILE>)"
                ]) {
                    test {
                        sql fileQuery("SELECT ${expression} FROM test_file_type_values")
                        exception "FILE"
                    }
                }
            }
        }
    }
    sql "SET enable_strict_cast = true"
    qt_try_cast fileQuery("""
        SELECT TRY_CAST(NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', CAST(-1 AS BIGINT),
                   'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE) IS NULL,
               CAST(NULL AS FILE) IS NULL
    """)
    qt_nested_try_cast fileQuery("""
        SELECT TRY_CAST(ARRAY(NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                              NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', CAST(-1 AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS ARRAY<FILE>),
               TRY_CAST(NAMED_STRUCT('good', NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                                     'bad', NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', CAST(-1 AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', NULL))
                        AS STRUCT<good:FILE,bad:FILE>),
               TRY_CAST(MAP('good', NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                            'bad', NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', CAST(-1 AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS MAP<STRING,FILE>)
    """)
    sql "SET enable_strict_cast = false"
    test {
        sql fileQuery("""SELECT CAST(NAMED_STRUCT('uri', 'urn:x', 'size', CAST(-1 AS BIGINT), 'offset', NULL,
                'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)""")
        exception "FILE size must be nonnegative"
    }
    sql "SET enable_strict_cast = true"

    for (def strict in [true, false]) {
        sql "SET enable_strict_cast = ${strict}"
        for (def targetType in ["ARRAY<FILE>", "MAP<STRING,FILE>", "STRUCT<f:FILE>",
                                "ARRAY<STRUCT<f:FILE>>", "STRUCT<f:ARRAY<FILE>>"]) {
            for (def source in ["'[]'", "PARSE_TO_VARIANT('[]')"]) {
                for (def castFunction in ["CAST", "TRY_CAST"]) {
                    test {
                        sql "SELECT ${castFunction}(${source} AS ${targetType})"
                        exception "cast"
                    }
                }
            }
        }
        test {
            sql fileQuery("SELECT CAST(files AS VARIANT) FROM test_file_type_nested")
            exception "cast"
        }
        test {
            // branch-4.1 rejects ARRAY<VARIANT> construction before the FILE cast.
            sql "SELECT CAST(ARRAY(PARSE_TO_VARIANT('[]')) AS ARRAY<ARRAY<FILE>>)"
            exception "array does not support jsonb/variant type"
        }
    }
    sql "SET enable_strict_cast = true"

    test {
        sql fileQuery("SELECT ELEMENT_AT(f, 'unknown') FROM test_file_type_values")
        exception "the specified field name unknown was not found"
    }

    for (def query in [
        "SELECT CAST('s3://bucket/file' AS FILE)",
        "SELECT ELEMENT_AT(f, id) FROM test_file_type_values",
        "SELECT ELEMENT_AT(f, CAST(id AS STRING)) FROM test_file_type_values",
        "SELECT f = f FROM test_file_type_values",
        "SELECT f IN (f) FROM test_file_type_values",
        "SELECT f FROM test_file_type_values ORDER BY f",
        "SELECT f, COUNT(*) FROM test_file_type_values GROUP BY f",
        "SELECT DISTINCT f FROM test_file_type_values",
        "SELECT COUNT(DISTINCT f) FROM test_file_type_values",
        "SELECT f FROM test_file_type_values UNION SELECT f FROM test_file_type_values",
        "SELECT DISTINCT files FROM test_file_type_nested",
        "SELECT files FROM test_file_type_nested ORDER BY files"
    ]) {
        test {
            sql fileQuery(query)
            exception(query.contains("f = f") ? "complex type"
                    : query.contains("COUNT(DISTINCT") ? "COUNT DISTINCT could not process type" : "FILE")
        }
    }
    // MIN's existing metric-type check rejects FILE before the shared capability check.
    test {
        sql fileQuery("SELECT MIN(f) FROM test_file_type_values")
        exception "must use with specific function"
    }

    // Explicit conversions remain query expressions; no FILE assignment policy is imposed.
    def structValue = "NAMED_STRUCT('uri', 'urn:cast', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL)"
    def fileValue = "CAST(${structValue} AS FILE)"
    def fileArrayValue = "CAST(ARRAY(${structValue}) AS ARRAY<FILE>)"
    qt_explicit_struct_to_file fileQuery("""
        SELECT CAST(TO_JSON(${fileValue}) AS STRING), TO_JSON(${fileValue}),
               ELEMENT_AT(${fileValue}, 'uri'),
               CAST(TO_JSON(ELEMENT_AT(${fileArrayValue}, 1)) AS STRING),
               ELEMENT_AT(ELEMENT_AT(${fileArrayValue}, 1), 'uri')
    """)
    for (def query in [
        "SELECT CAST(f AS STRING) FROM test_file_type_values",
        "SELECT CAST(NAMED_STRUCT('uri', 'urn:partial') AS FILE)",
        "SELECT IF(id = 2, JSON_PARSE('{\"uri\":\"urn:mixed\"}'), f) FROM test_file_type_values",
        "SELECT COALESCE(f, NAMED_STRUCT('uri', 'urn:mixed')) FROM test_file_type_values"
    ]) {
        test {
            sql fileQuery(query)
            exception "FILE"
        }
    }
    qt_explicit_forward fileQuery("""
        SELECT id, ELEMENT_AT(CAST(j AS FILE), 'uri'),
               ELEMENT_AT(ELEMENT_AT(CAST(ARRAY(j) AS ARRAY<FILE>), 1), 'uri')
        FROM (
            SELECT 2 AS id, NAMED_STRUCT('uri', 'urn:struct', 'offset', NULL, 'size', NULL,
                'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS j
            UNION ALL SELECT 3, NAMED_STRUCT('uri', 'urn:reference', 'offset', NULL, 'size', NULL,
                'content_type', NULL, 'checksum', NULL, 'inline', NULL)
        ) scalar_cast_rows ORDER BY id
    """)

    sql "DROP TABLE IF EXISTS test_file_type_invalid"
    for (def definition in [
        "(f FILE, id INT) DUPLICATE KEY(f) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "(id INT, f FILE) DUPLICATE KEY(id) DISTRIBUTED BY HASH(f) BUCKETS 1",
        "(id INT, f MAP<FILE,STRING>) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1",
        "(id INT, f FILE, INDEX idx_f(f) USING INVERTED) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"
    ]) {
        test {
            sql "CREATE TABLE test_file_type_invalid ${definition} PROPERTIES('replication_num'='1')"
            exception "FILE"
        }
    }
    for (def kind in ["FUNCTION", "AGGREGATE FUNCTION", "TABLES FUNCTION"]) {
        [["FILE", "INT"], ["ARRAY<FILE>", "INT"],
         ["INT", "FILE"], ["INT", "STRUCT<asset:FILE>"]].eachWithIndex { signature, index ->
            def resultType = kind == "TABLES FUNCTION" ? "ARRAY<${signature[1]}>" : signature[1]
            def functionName = "test_file_python_${kind.replace(' ', '_').toLowerCase()}_${index}"
            sql "DROP FUNCTION IF EXISTS ${functionName}(${signature[0]})"
            sql """
                    CREATE ${kind} ${functionName}(${signature[0]}) RETURNS ${resultType}
                    PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="3.12.11")
                    AS \$\$
def evaluate(value):
    return value
\$\$
                """
        }
    }

    qt_inline_values fileQuery("""
        SELECT id, f, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'size'),
               FROM_BINARY(FL_GET_BINARY(f)), f IS NULL
        FROM test_file_type_inline_values ORDER BY id
    """)
    qt_inline_counts fileQuery("""
        SELECT COUNT(f), COUNT(ELEMENT_AT(f, 'uri')), COUNT(FL_GET_BINARY(f))
        FROM test_file_type_inline_values
    """)
    qt_inline_constructors fileQuery("""
        SELECT id, ARRAY(f, NULL), NAMED_STRUCT('asset', f), MAP('asset', f),
               FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(ARRAY(f), 1))),
               FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(NAMED_STRUCT('asset', f), 'asset'))),
               FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(MAP('asset', f), 'asset')))
        FROM test_file_type_inline_values ORDER BY id
    """)
    qt_inline_explode fileQuery("""
        SELECT id, uri, off, size, mime, checksum, FROM_BINARY(inline_bytes)
        FROM test_file_type_inline_values
        LATERAL VIEW EXPLODE_FILE(f) e AS uri, off, size, mime, checksum, inline_bytes
        ORDER BY id
    """)
    def inlineStrictCast = sql("SELECT @@enable_strict_cast")[0][0]
    sql "SET enable_strict_cast = true"
    try {
        test {
            sql fileQuery("""SELECT CAST(NAMED_STRUCT('uri', NULL, 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)""")
            exception "FILE uri must not be NULL"
        }
        qt_inline_with_raw_uri fileQuery("""SELECT CAST(NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', X'') AS FILE)""")
    } finally {
        sql "SET enable_strict_cast = ${inlineStrictCast}"
    }
}
