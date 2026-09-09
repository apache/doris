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

suite("test_uuid_cast_matrix", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "DROP TABLE IF EXISTS uuid_matrix_cast"
    sql """CREATE TABLE uuid_matrix_cast (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_cast VALUES ${matrix.values()}"

    sql "DROP TABLE IF EXISTS uuid_matrix_cast_text"
    sql """CREATE TABLE uuid_matrix_cast_text (id INT,s STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    List<String> texts = ["NULL", "'00000000-0000-0000-0000-000000000000'", "'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'",
        "'550E8400E29B41D4A716446655440000'", "'018f0f59-1010-7abc-9234-001122334455'",
        "'7fffffff-ffff-ffff-ffff-ffffffffffff'", "'80000000000000000000000000000000'",
        "''", "'invalid'", "'{550e8400-e29b-41d4-a716-446655440000}'",
        "' 550e8400-e29b-41d4-a716-446655440000'", "'550e8400-e29b-41d4-a716-446655440000 '",
        "'550e8400-e29b-41d4-a716446655440000'", "'550e8400-e29b-41d4-a716-44665544000g'",
        "'0000000000000000000000000000000'", "'000000000000000000000000000000000'",
        "CONCAT('550e8400-e29b-41d4-a716-446655440000',CHAR(0))", "'非UUID'"]
    List<Map> rows = texts.collect { [s: "CONCAT(${it},'')"] }
    sql "INSERT INTO uuid_matrix_cast_text VALUES ${rows.withIndex().collect { r,i -> "(${i},${r.s})" }.join(',')}"
    sql "SET enable_strict_cast=false"
    matrix.run(delegate, 'text', 'uuid_matrix_cast_text', ['s'], { s ->
        [cast_uuid: "CAST(${s} AS UUID)", try_uuid: "TRY_CAST(${s} AS UUID)",
         char_uuid: "CAST(CAST(${s} AS CHAR(64)) AS UUID)", varchar_uuid: "CAST(CAST(${s} AS VARCHAR(64)) AS UUID)",
         variant_uuid: "CAST(CAST(${s} AS VARIANT) AS UUID)"]
    }, [rows: rows])
    matrix.run(delegate, 'uuid', 'uuid_matrix_cast', ['u'], { u ->
        [identity_value: "CAST(${u} AS UUID)", string_value: "CAST(${u} AS STRING)",
         char_value: "CAST(${u} AS CHAR(36))", varchar_value: "CAST(${u} AS VARCHAR(36))",
         variant_roundtrip: "CAST(CAST(${u} AS VARIANT) AS UUID)",
         string_roundtrip: "CAST(CAST(${u} AS STRING) AS UUID)",
         try_roundtrip: "TRY_CAST(CAST(${u} AS STRING) AS UUID)"]
    })
    matrix.run(delegate, 'array', 'uuid_matrix_cast', ['a'], { a ->
        [text_value: "CAST(${a} AS STRING)", array_strings: "CAST(${a} AS ARRAY<STRING>)",
         roundtrip: "CAST(CAST(${a} AS ARRAY<STRING>) AS ARRAY<UUID>)"]
    })
    matrix.run(delegate, 'map', 'uuid_matrix_cast', ['m'], { m ->
        [text_value: "CAST(${m} AS STRING)", roundtrip: "CAST(CAST(${m} AS MAP<STRING,STRING>) AS MAP<UUID,UUID>)"]
    })
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        sql "SET enable_strict_cast=true"
        for (int i = 7; i < rows.size(); ++i) {
            for (String input : [rows[i].s, 's']) {
                test {
                    sql "SELECT CAST(${input} AS UUID) FROM uuid_matrix_cast_text WHERE id=${i}"
                    exception "uuid"
                }
                qt_strict_try "SELECT TRY_CAST(${input} AS UUID) FROM uuid_matrix_cast_text WHERE id=${i}"
            }
        }
        qt_strict_valid "SELECT id,CAST(s AS UUID),TRY_CAST(s AS UUID) FROM uuid_matrix_cast_text WHERE id<7 ORDER BY id"
    }
}
