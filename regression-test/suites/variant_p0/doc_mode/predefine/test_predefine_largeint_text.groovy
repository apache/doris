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

// Integers beyond 64 bits on the LARGEINT paths of a doc-mode Schema Template. Doc mode takes a
// LARGEINT template field (it refuses a DECIMAL one), and its document column keeps each value as
// the parser read it, before the template's cast: up to 10^38 - 1 an exact number, above that its
// digits, quoted in the document text. A path is read from its materialized subcolumn when there is
// one (test_predefine_largeint_doc_mat), else from the document (test_predefine_largeint_doc).
// Each document is loaded by INSERT ... SELECT parse_to_variant (its id), INSERT ... SELECT
// try_parse_to_variant (1000 + id) and Stream Load (2000 + id): the rows of one document must read
// alike.
suite("test_predefine_largeint_text_doc_mode", "p0") {
    def docs = [
        // 2^64 and -2^63 - 1 fit no 64-bit type
        [1, '{"id":18446744073709551616}'],
        [2, '{"id":-9223372036854775809}'],
        // 10^38 - 1, the largest integer a Variant decimal16 holds, then 10^38 above it
        [3, '{"id":99999999999999999999999999999999999999}'],
        [4, '{"id":100000000000000000000000000000000000000}'],
        // 2^127 - 1 and -2^127, the LARGEINT bounds, and 2^127, which no LARGEINT holds
        [5, '{"id":170141183460469231731687303715884105727}'],
        [6, '{"id":-170141183460469231731687303715884105728}'],
        [7, '{"id":170141183460469231731687303715884105728}'],
        // the string form of 2^127 - 1
        [8, '{"id":"170141183460469231731687303715884105727"}'],
        // one ARRAY<LARGEINT> holding an int64 and an integer above 10^38 - 1
        [9, '{"ids":[1,170141183460469231731687303715884105727]}'],
        [10, '{"id":7}'],
    ]

    sql "DROP TABLE IF EXISTS test_predefine_largeint_doc_src"
    sql """
        CREATE TABLE `test_predefine_largeint_doc_src` (
            `id` bigint NOT NULL,
            `j` string NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """
    sql "insert into test_predefine_largeint_doc_src values " +
            docs.collect { doc -> "(${doc[0]}, '${doc[1]}')" }.join(", ")

    ["test_predefine_largeint_doc": "1000000", "test_predefine_largeint_doc_mat": "0"].each { table_name, min_rows ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE `${table_name}` (
                `id` bigint NOT NULL,
                `v` variant<
                    match_name 'id' : largeint,
                    match_name 'ids' : array<largeint>,
                    properties("variant_enable_doc_mode" = "true", "variant_doc_materialization_min_rows" = "${min_rows}")
                > NULL
            ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
        """
        sql "insert into ${table_name} select id, parse_to_variant(j) from test_predefine_largeint_doc_src"
        sql "insert into ${table_name} select id + 1000, try_parse_to_variant(j) from test_predefine_largeint_doc_src"
        streamLoad {
            table "${table_name}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'max_filter_ratio', '0'
            inputText docs.collect { doc -> "{\"id\":${2000 + doc[0]},\"v\":${doc[1]}}" }.join("\n")
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                logger.info("Stream load into ${table_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(docs.size(), json.NumberLoadedRows as int)
            }
        }

        quickTest("${table_name}_document", "select id, cast(v as string) from ${table_name} order by id", false)
        quickTest("${table_name}_id", """select id, cast(v['id'] as largeint), cast(v['id'] as string)
                from ${table_name} order by id""", false)
        quickTest("${table_name}_ids", """select id, cast(v['ids'] as array<largeint>), cast(v['ids'] as string)
                from ${table_name} where id % 1000 = 9 order by id""", false)
        quickTest("${table_name}_eq", """select id from ${table_name}
                where cast(v['id'] as largeint) = 170141183460469231731687303715884105727 order by id""", false)
    }
}
