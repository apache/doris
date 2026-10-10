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
    sql "DROP TABLE IF EXISTS test_file_type_values"
    sql """
        CREATE TABLE test_file_type_values (id INT NOT NULL, f FILE NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_values VALUES
        (1, CAST(JSON_PARSE('{"uri":"S3://Bucket/a%2fb?versionId=v1","size":0,
             "content_type":"Image/PNG","checksum":"CRC32:00000000","offset":null,"inline":"AP+A"}') AS FILE)),
        (2, NULL),
        (3, CAST(NAMED_STRUCT('SIZE', CAST(CAST(8 AS SMALLINT) AS BIGINT), 'URI', 'urn:example:three', 'OFFSET', CAST(CAST(2 AS INT) AS BIGINT), 'content_type', NULL, 'checksum', NULL, 'inline', X'') AS FILE))
    """
    for (def fileCell in ['{"uri":"urn:csv"}', '\\N']) {
        streamLoad {
            table "test_file_type_values"
            set "format", "csv"
            set "columns", "id,f"
            set "column_separator", "\t"
            inputText "10\t${fileCell}\n"
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def response = parseJson(result)
                assertEquals("fail", response.Status.toLowerCase(), result)
                assertTrue(response.Message.contains("CSV") && response.Message.contains("FILE"), result)
            }
        }
    }
    qt_metadata "DESC test_file_type_values"
    qt_values "SELECT id, f, CAST(CAST(f AS JSON) AS STRING), CAST(f AS JSON) FROM test_file_type_values ORDER BY id"
    qt_fields """
        SELECT id, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'), ELEMENT_AT(f, 'size'),
               ELEMENT_AT(f, 'content_type'), ELEMENT_AT(f, 'checksum'), ELEMENT_AT(f, 'UrI'),
               HEX(CAST(ELEMENT_AT(f, 'inline') AS STRING)), HEX(CAST(ELEMENT_AT(f, 'INLINE') AS STRING)),
               ELEMENT_AT(f, 'inline') IS NULL,
               f IS NULL, f IS NOT NULL
        FROM test_file_type_values ORDER BY id
    """
    qt_struct """
        SELECT id, CAST(f AS STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT,
                   content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>)
        FROM test_file_type_values ORDER BY id
    """
    qt_count "SELECT COUNT(f) FROM test_file_type_values"
    qt_ordinary_functions """
        SELECT ARRAY_MAP(x -> x + 1, ARRAY(2, 1)), ARRAY_SORTBY(ARRAY('b', 'a'), ARRAY(2, 1)),
               COALESCE(NULL, 9), IF(TRUE, 'x', 'y'), NAMED_STRUCT('k', 5), MAP('k', 5)
    """
    qt_count_null_input "SELECT COUNT(f), COUNT(*) FROM test_file_type_values WHERE f IS NULL"
    qt_count_window """
        SELECT id, COUNT(f) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        FROM test_file_type_values ORDER BY id
    """
    qt_count_empty_containers """
        SELECT COUNT(files), COUNT(lookup)
        FROM (SELECT CAST(ARRAY() AS ARRAY<FILE>) files, CAST(MAP() AS MAP<STRING,FILE>) lookup) t
    """
    qt_count_fields """
        SELECT COUNT(ELEMENT_AT(f, 'uri')), COUNT(ELEMENT_AT(f, 'size')),
               COUNT(ELEMENT_AT(f, 'inline')) FROM test_file_type_values
    """
    for (def aggregate in ["ANY_VALUE(f)", "ARRAY_AGG(f)", "COLLECT_LIST(f)",
                          "COLLECT_SET(f)", "MIN_BY(f, id)", "MAX_BY(f, id)", "MAP_AGG(id, f)",
                          "ANY_VALUE(ARRAY(f))", "ARRAY_AGG(NAMED_STRUCT('asset', f))"]) {
        test {
            sql "SELECT ${aggregate} FROM test_file_type_values"
            exception "FILE"
        }
    }
    // Correlated scalar subqueries are rewritten to ANY_VALUE; FILE is rejected there too.
    test {
        sql """
            SELECT (SELECT b.f FROM test_file_type_values b WHERE b.id = a.id)
            FROM test_file_type_values a
        """
        exception "does not support FILE"
    }
    def originalAggState = sql("SELECT @@enable_agg_state")[0][0]
    sql "SET enable_agg_state = true"
    try {
        for (def statement in [
            "SELECT ANY_VALUE_STATE(f) FROM test_file_type_values",
            "SELECT COUNT_STATE(f) FROM test_file_type_values",
            "SELECT CAST(NULL AS AGG_STATE<any_value(FILE)>)",
            "ALTER TABLE test_file_type_values ADD COLUMN st AGG_STATE<any_value(FILE)>"
        ]) {
            test {
                sql statement
                exception "FILE"
            }
        }
    } finally {
        sql "SET enable_agg_state = ${originalAggState}"
    }
    qt_scalar_group "SELECT ELEMENT_AT(f, 'size'), COUNT(*) FROM test_file_type_values GROUP BY ELEMENT_AT(f, 'size') ORDER BY 1"
    qt_scalar_join """
        SELECT a.id, b.f FROM test_file_type_values a JOIN test_file_type_values b
        ON a.id = b.id ORDER BY a.id
    """
    qt_window_payload "SELECT id, f, ROW_NUMBER() OVER (ORDER BY id) FROM test_file_type_values ORDER BY id"
    qt_window_scalar_fields """
        SELECT id, LAG(ELEMENT_AT(f, 'uri')) OVER (ORDER BY id),
               LEAD(ELEMENT_AT(f, 'size')) OVER (ORDER BY id),
               FIRST_VALUE(ELEMENT_AT(f, 'uri')) OVER (ORDER BY id),
               LAST_VALUE(ELEMENT_AT(f, 'size')) OVER (ORDER BY id)
        FROM test_file_type_values ORDER BY id
    """
    for (def expression in [
        "FIRST_VALUE(f) OVER (ORDER BY id)",
        "LAST_VALUE(f) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
        "NTH_VALUE(f, 1) OVER (ORDER BY id)",
        "LAG(f) OVER (ORDER BY id)", "LEAD(f) OVER (ORDER BY id)",
        "IF(id = 2, CAST(NULL AS FILE), f)", "COALESCE(f, CAST(NULL AS FILE))",
        "IFNULL(f, CAST(NULL AS FILE))", "NULLIF(f, f)",
        "ARRAY(f, NULL)", "NAMED_STRUCT('f', f)", "MAP('source', f)"
    ]) {
        test {
            sql "SELECT ${expression} FROM test_file_type_values"
            exception "FILE"
        }
    }
    sql "DROP FUNCTION IF EXISTS test_file_alias_size(FILE)"
    sql """CREATE ALIAS FUNCTION test_file_alias_size(FILE)
           WITH PARAMETER(f) AS ELEMENT_AT(f, 'size')"""
    for (def expression in ["test_file_alias_size(NULL)", "test_file_alias_size(f)",
                           "ARRAY_MAP(x -> f, ARRAY(id))"]) {
        test {
            sql "SELECT ${expression} FROM test_file_type_values"
            exception "FILE"
        }
    }
    qt_union_all """
        SELECT id, f FROM test_file_type_values
        UNION ALL SELECT 4, CAST(NAMED_STRUCT('uri', 'urn:four', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)
        ORDER BY id
    """
    qt_explode """
        SELECT id, uri, off, len, mime, digest, HEX(CAST(data AS STRING)) FROM test_file_type_values
        LATERAL VIEW EXPLODE_FILE(f) e AS uri, off, len, mime, digest, data ORDER BY id
    """
    qt_explode_outer """
        SELECT id, uri, off, len, mime, digest, HEX(CAST(data AS STRING)) FROM test_file_type_values
        LATERAL VIEW EXPLODE_FILE_OUTER(f) e AS uri, off, len, mime, digest, data ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_file_type_nested"
    sql """
        CREATE TABLE test_file_type_nested (
            id INT NOT NULL, files ARRAY<FILE>, holder STRUCT<f:FILE>, lookup MAP<STRING,FILE>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_nested
        SELECT id, CAST(ARRAY(CAST(f AS ${publicStruct}), NULL) AS ARRAY<FILE>),
               CAST(NAMED_STRUCT('f', CAST(f AS ${publicStruct})) AS STRUCT<f:FILE>),
               CAST(MAP('source', CAST(f AS ${publicStruct})) AS MAP<STRING,FILE>)
        FROM test_file_type_values
    """
    qt_nested_getters """
        SELECT id, ELEMENT_AT(ELEMENT_AT(files, 1), 'uri'),
               ELEMENT_AT(ELEMENT_AT(holder, 'f'), 'uri'), ELEMENT_AT(ELEMENT_AT(lookup, 'source'), 'uri')
        FROM test_file_type_nested ORDER BY id
    """
    qt_nested_payload "SELECT * FROM test_file_type_nested ORDER BY id"
    qt_nested_union_all """
        SELECT id, files, holder, lookup FROM test_file_type_nested
        UNION ALL SELECT 4, CAST(NULL AS ARRAY<FILE>), CAST(NULL AS STRUCT<f:FILE>),
                         CAST(NULL AS MAP<STRING,FILE>)
        ORDER BY id
    """
    qt_nested_nulls """
        SELECT id, ELEMENT_AT(files, 2) IS NULL, ELEMENT_AT(holder, 'f') IS NULL
        FROM test_file_type_nested ORDER BY id
    """
    qt_nested_count "SELECT COUNT(files), COUNT(holder), COUNT(lookup) FROM test_file_type_nested"
    for (def expression in [
        "REVERSE(files)", "ARRAY_SLICE(files, 1, 1)", "ARRAY_CONCAT(files, files)",
        "ARRAY_SORTBY(files, ARRAY(2, 1))", "ARRAY_MAP(x -> ELEMENT_AT(x, 'uri'), files)",
        "MAP_VALUES(lookup)", "ARRAY_SIZE(files)", "MAP_SIZE(lookup)",
        "LAG(files) OVER (ORDER BY id)", "LEAD(holder) OVER (ORDER BY id)",
        "FIRST_VALUE(lookup) OVER (ORDER BY id)", "LAST_VALUE(files) OVER (ORDER BY id)",
        "NTH_VALUE(holder, 1) OVER (ORDER BY id)"
    ]) {
        test {
            sql "SELECT ${expression} FROM test_file_type_nested"
            exception "FILE"
        }
    }
    test {
        sql """
            SELECT id, file_value FROM test_file_type_nested
            LATERAL VIEW EXPLODE(files) e AS file_value
        """
        exception "FILE"
    }

    sql "SET enable_strict_cast = true"
    for (def invalid in [
        '{"uri":"urn:missing-inline","offset":null,"size":null,"content_type":null,"checksum":null}',
        '{"uri":"urn:bad-inline","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AB=="}',
        '{"uri":"urn:bad-inline","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AA"}',
        '{"uri":"relative/path","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}', '{"uri":"s3://bucket/file#fragment","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}',
        '{"uri":"s3://bucket/%ZZ","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}', '{"uri":"urn:x","offset":1,"size":null,"content_type":null,"checksum":null,"inline":null}',
        '{"uri":"urn:x","size":-1,"offset":null,"content_type":null,"checksum":null,"inline":null}',
        '{"uri":"urn:x","offset":9223372036854775807,"size":1,"content_type":null,"checksum":null,"inline":null}',
        '{"uri":"urn:x","size":1.5,"offset":null,"content_type":null,"checksum":null,"inline":null}', '{"uri":"urn:x","content_type":"invalid","offset":null,"size":null,"checksum":null,"inline":null}',
        '{"uri":"urn:x","checksum":"md5:00000000000000000000000000000000","offset":null,"size":null,"content_type":null,"inline":null}',
        '{"uri":"urn:x","checksum":"MD5:ABCDEF00000000000000000000000000","offset":null,"size":null,"content_type":null,"inline":null}',
        '{"uri":"urn:x","inline":7,"offset":null,"size":null,"content_type":null,"checksum":null}', '{"URI":"urn:x","uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}',
        '{"uri":"urn:x","uri":"urn:y","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}', '{"uri":"urn:x","extra":1,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}'
    ]) {
        test {
            sql "SELECT CAST(JSON_PARSE('${invalid}') AS FILE)"
            exception "FILE"
        }
    }
    qt_try_cast """
        SELECT TRY_CAST(JSON_PARSE('{"uri":"relative/path","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}') AS FILE) IS NULL,
               CAST(JSON_PARSE('null') AS FILE) IS NULL, CAST(NULL AS FILE) IS NULL
    """
    qt_nested_try_cast """
        SELECT TRY_CAST(ARRAY(NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                              NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS ARRAY<FILE>),
               TRY_CAST(NAMED_STRUCT('good', NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                                     'bad', NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL))
                        AS STRUCT<good:FILE,bad:FILE>),
               TRY_CAST(MAP('good', NAMED_STRUCT('uri', 'urn:valid', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL),
                            'bad', NAMED_STRUCT('uri', 'relative/path', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS MAP<STRING,FILE>)
    """
    sql "SET enable_strict_cast = false"
    qt_atomic_null """SELECT CAST(JSON_PARSE('{"uri":"urn:x","size":-1,"offset":null,"content_type":null,"checksum":null,"inline":null}') AS FILE) IS NULL"""
    sql "SET enable_strict_cast = true"

    for (def query in [
        "SELECT CAST('s3://bucket/file' AS FILE)",
        "SELECT ELEMENT_AT(f, 'unknown') FROM test_file_type_values",
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
        "SELECT files FROM test_file_type_nested ORDER BY files",
        "SELECT MAP(f, id) FROM test_file_type_values"
    ]) {
        test {
            sql query
            exception "FILE"
        }
    }
    // MIN's existing metric-type check rejects FILE before the shared capability check.
    test {
        sql "SELECT MIN(f) FROM test_file_type_values"
        exception "must use with specific function"
    }

    sql "DROP TABLE IF EXISTS test_file_type_assignment_casts"
    sql """
        CREATE TABLE test_file_type_assignment_casts (
            id INT, s STRING, j JSON,
            p STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT,
                     content_type:VARCHAR(1024),checksum:VARCHAR(1024)>,
            a_s ARRAY<STRING>,
            a_p ARRAY<STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT,
                            content_type:VARCHAR(1024),checksum:VARCHAR(1024)>>,
            f FILE, files ARRAY<FILE>, n INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    def fileValue = "CAST(NAMED_STRUCT('uri', 'urn:assignment', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)"
    def fileArrayValue = "CAST(ARRAY(CAST(${fileValue} AS ${publicStruct})) AS ARRAY<FILE>)"
    for (def column in ["s", "j", "p"]) {
        test {
            sql "INSERT INTO test_file_type_assignment_casts(id, ${column}) VALUES (0, ${fileValue})"
            exception "Cannot implicitly convert"
        }
    }
    // ARRAY<JSON> table columns are unsupported; that target is covered by FE tests.
    for (def column in ["a_s", "a_p"]) {
        test {
            sql "INSERT INTO test_file_type_assignment_casts(id, ${column}) VALUES (0, ${fileArrayValue})"
            exception "Cannot implicitly convert"
        }
    }
    test {
        sql """INSERT INTO test_file_type_assignment_casts(id, s)
               VALUES (0, COALESCE(CAST(NULL AS FILE), CAST(NULL AS FILE)))"""
        exception "FILE"
    }
    test {
        sql "INSERT INTO test_file_type_assignment_casts(id, s) SELECT 0, ${fileValue}"
        exception "Cannot implicitly convert"
    }
    sql """
        INSERT INTO test_file_type_assignment_casts(id, s, j, a_s) VALUES
        (1, CAST(CAST(${fileValue} AS JSON) AS STRING), CAST(${fileValue} AS JSON),
         CAST(CAST(${fileArrayValue} AS ARRAY<JSON>) AS ARRAY<STRING>))
    """
    qt_assignment_explicit """
        SELECT id, s, j, ELEMENT_AT(CAST(${fileValue} AS ${publicStruct}), 'uri'), ELEMENT_AT(a_s, 1),
               ELEMENT_AT(ELEMENT_AT(CAST(${fileArrayValue} AS ARRAY<${publicStruct}>), 1), 'uri')
        FROM test_file_type_assignment_casts WHERE id = 1 ORDER BY id
    """
    sql """
        INSERT INTO test_file_type_assignment_casts(id, f, files) VALUES
        (2, CAST(NAMED_STRUCT('uri', 'urn:struct', 'offset', NULL, 'size', NULL,
                             'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE),
            CAST(ARRAY(NAMED_STRUCT('uri', 'urn:struct', 'offset', NULL, 'size', NULL,
                                   'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS ARRAY<FILE>)),
        (3, CAST(JSON_PARSE('{"uri":"urn:json","offset":null,"size":null,
                             "content_type":null,"checksum":null,"inline":null}') AS FILE),
            CAST(ARRAY(NAMED_STRUCT('uri', 'urn:json', 'offset', NULL, 'size', NULL,
                                   'content_type', NULL, 'checksum', NULL, 'inline', NULL)) AS ARRAY<FILE>))
    """
    for (def source in [
        "NAMED_STRUCT('uri', 'urn:struct', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL)",
        "JSON_PARSE('{\"uri\":\"urn:json\",\"offset\":null,\"size\":null,\"content_type\":null,\"checksum\":null,\"inline\":null}')"
    ]) {
        test {
            sql "INSERT INTO test_file_type_assignment_casts(id, f) VALUES (0, ${source})"
            exception "Cannot implicitly convert"
        }
    }
    for (def query in [
        "SELECT CAST(f AS STRING) FROM test_file_type_values",
        "SELECT CAST(NAMED_STRUCT('uri', 'urn:partial') AS FILE)",
        "SELECT IF(id = 2, JSON_PARSE('{\"uri\":\"urn:mixed\"}'), f) FROM test_file_type_values",
        "SELECT COALESCE(f, NAMED_STRUCT('uri', 'urn:mixed')) FROM test_file_type_values"
    ]) {
        test {
            sql query
            exception "FILE"
        }
    }
    qt_variant_casts """
        SELECT id, ELEMENT_AT(CAST(CAST(f AS VARIANT) AS FILE), 'uri'),
               ELEMENT_AT(CAST(CAST(f AS VARIANT) AS FILE), 'size'),
               HEX(CAST(ELEMENT_AT(CAST(CAST(f AS VARIANT) AS FILE), 'inline') AS STRING)),
               HEX(CAST(ELEMENT_AT(CAST(CAST(f AS JSON) AS FILE), 'inline') AS STRING))
        FROM test_file_type_values ORDER BY id
    """
    qt_inline_struct_roundtrip """
        SELECT id, HEX(CAST(ELEMENT_AT(CAST(CAST(f AS
                   STRUCT<inline:VARBINARY,checksum:STRING,content_type:STRING,size:BIGINT,offset:BIGINT,uri:STRING>)
                   AS FILE), 'inline') AS STRING))
        FROM test_file_type_values ORDER BY id
    """
    qt_assignment_forward """
        SELECT id, ELEMENT_AT(f, 'uri'), ELEMENT_AT(ELEMENT_AT(files, 1), 'uri')
        FROM test_file_type_assignment_casts WHERE id IN (2, 3) ORDER BY id
    """
    sql """INSERT INTO test_file_type_assignment_casts(id, s, n) VALUES (4, ABS(-7), CONCAT('1', '2'))"""
    qt_assignment_ordinary "SELECT id, s, n FROM test_file_type_assignment_casts WHERE id = 4 ORDER BY id"

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
    // FILE fields have one SQL access API: ELEMENT_AT. Former getters are unregistered.
    for (def functionName in ["fl_get_uri", "fl_get_offset", "fl_get_size",
                             "fl_get_content_type", "fl_get_checksum", "fl_get_inline"]) {
        test {
            sql "SELECT ${functionName}(f) FROM test_file_type_values"
            exception functionName
        }
    }

    // Reject FILE signatures at creation, before any Python worker is needed.
    for (def kind in ["FUNCTION", "AGGREGATE FUNCTION", "TABLES FUNCTION"]) {
        for (def signature in [["FILE", "INT"], ["ARRAY<FILE>", "INT"],
                               ["INT", "FILE"], ["INT", "STRUCT<asset:FILE>"]]) {
            def resultType = kind == "TABLES FUNCTION" ? "ARRAY<${signature[1]}>" : signature[1]
            test {
                sql """
                    CREATE ${kind} test_file_python_unsupported(${signature[0]}) RETURNS ${resultType}
                    PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="3.12.11")
                    AS \$\$
def evaluate(value):
    return value
\$\$
                """
                exception "does not support"
            }
        }
    }

}
