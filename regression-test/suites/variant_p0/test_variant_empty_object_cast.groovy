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

// A VARIANT row holding an empty JSON object carries no payload: it has no subcolumns and its
// root never gets a type. A column in which every row looks like that is still a "scalar"
// variant, and casting it to STRING/JSON used to take the scalar-root fast path, fail to convert
// the untyped root and return NULL for every row - while the very same value sitting in a column
// that also holds paths came back as `{}`.
// See https://github.com/apache/doris/issues/67367.
suite("test_variant_empty_object_cast", "variant_type") {
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""

    def createTable = { String name ->
        sql "DROP TABLE IF EXISTS ${name}"
        sql """
            CREATE TABLE ${name} (
                k INT,
                v VARIANT NULL,
                vn VARIANT NOT NULL
            ) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
    }

    // Every row is an empty object, so the column has no path at all.
    def onlyEmpty = "variant_only_empty_object"
    createTable(onlyEmpty)
    sql """INSERT INTO ${onlyEmpty} VALUES (1, ${variantV2Function}('{}'), ${variantV2Function}('{}'))"""

    order_qt_only_empty_raw "SELECT k, v, vn FROM ${onlyEmpty}"
    order_qt_only_empty_cast_string "SELECT k, CAST(v AS STRING), CAST(vn AS STRING) FROM ${onlyEmpty}"
    order_qt_only_empty_cast_json "SELECT k, CAST(v AS JSON), CAST(vn AS JSON) FROM ${onlyEmpty}"
    order_qt_only_empty_cast_length "SELECT k, LENGTH(CAST(v AS STRING)), LENGTH(CAST(vn AS STRING)) FROM ${onlyEmpty}"

    // A SQL NULL is not an empty object and must keep casting to NULL.
    def withNull = "variant_empty_object_with_null"
    createTable(withNull)
    sql """INSERT INTO ${withNull} VALUES
            (1, ${variantV2Function}('{}'), ${variantV2Function}('{}')),
            (2, NULL, ${variantV2Function}('{}'))"""

    order_qt_with_null_raw "SELECT k, v, vn FROM ${withNull}"
    order_qt_with_null_cast_string "SELECT k, CAST(v AS STRING), CAST(vn AS STRING) FROM ${withNull}"
    order_qt_with_null_is_null "SELECT k, v IS NULL, vn IS NULL FROM ${withNull}"

    // Control: the same empty object next to a row that does carry a path. This shape has always
    // rendered as `{}`, and both shapes have to agree.
    def mixed = "variant_mixed_empty_object"
    createTable(mixed)
    sql """INSERT INTO ${mixed} VALUES
            (1, ${variantV2Function}('{}'), ${variantV2Function}('{}')),
            (2, ${variantV2Function}('{"a" : 1}'), ${variantV2Function}('{"a" : 1}'))"""

    order_qt_mixed_raw "SELECT k, v, vn FROM ${mixed}"
    order_qt_mixed_cast_string "SELECT k, CAST(v AS STRING), CAST(vn AS STRING) FROM ${mixed}"

    // The empty object reads back the same whether or not it shares a column with real paths.
    def emptyRowOfOnlyEmpty = sql "SELECT CAST(v AS STRING), CAST(vn AS STRING) FROM ${onlyEmpty} WHERE k = 1"
    def emptyRowOfMixed = sql "SELECT CAST(v AS STRING), CAST(vn AS STRING) FROM ${mixed} WHERE k = 1"
    assertEquals(emptyRowOfMixed.toString(), emptyRowOfOnlyEmpty.toString())
}
