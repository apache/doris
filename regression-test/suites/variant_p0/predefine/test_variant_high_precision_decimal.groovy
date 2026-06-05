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

suite("test_variant_high_precision_decimal", "p0") {
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set enable_decimal256 = true """

    def decimal256Integer = "99999999999999999999999999999999999999999999999999"

    sql "DROP TABLE IF EXISTS test_variant_high_precision_decimal"
    sql "DROP TABLE IF EXISTS test_variant_high_precision_decimal_json_stage"
    sql """
        CREATE TABLE test_variant_high_precision_decimal (
            `id` bigint NOT NULL,
            `v` variant<
                'number_1':decimalv3(38,10),
                'number_2':decimalv3(38,10),
                'number_3':decimalv3(38,10),
                'number_4':decimalv3(38,18),
                'number_big_integer':decimalv3(76,0),
                'number_scientific_1':decimalv3(38,10),
                'number_scientific_2':decimalv3(38,10),
                'numberArray_1':array<decimalv3(38,10)>,
                'glob_decimal_*':decimalv3(38,10),
                'string_first_*':string,
                'string_first_decimal':decimalv3(38,10),
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "1"
                )
            > NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """
    sql """
        CREATE TABLE test_variant_high_precision_decimal_json_stage (
            `id` bigint NOT NULL,
            `v` string NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO test_variant_high_precision_decimal VALUES
        (
            1,
            '{
                "number_1": 999999999999999999999999999.999999999,
                "number_2": 0.0000000001,
                "number_3": -999999999999999999999999999.999999999,
                "number_4": 0.57,
                "number_big_integer": ${decimal256Integer},
                "number_scientific_1": 999999999999999999999999999.999999999e0,
                "number_scientific_2": 1e-10,
                "numberArray_1": [
                    999999999999999999999999999.999999999,
                    -999999999999999999999999999.999999999
                ],
                "glob_decimal_1": 999999999999999999999999999.999999999,
                "string_first_decimal": 999999999999999999999999999.999999999,
                "untyped_decimal": 999999999999999999999999999.999999999,
                "untyped_big_integer": 340282366920938463463374607431768211456
            }'
        )
    """
    sql "sync"

    streamLoad {
        table "test_variant_high_precision_decimal_json_stage"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        set 'max_filter_ratio', '0'
        file "test_variant_high_precision_decimal_stream.json"
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            logger.info("Stream load test_variant_high_precision_decimal_stream.json result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberLoadedRows as int)
        }
    }
    sql """
        INSERT INTO test_variant_high_precision_decimal
        SELECT id, v FROM test_variant_high_precision_decimal_json_stage
    """
    sql "sync"

    streamLoad {
        table "test_variant_high_precision_decimal"
        set 'read_json_by_line', 'true'
        set 'format', 'json'
        set 'max_filter_ratio', '0'
        file "test_variant_high_precision_decimal_stream.json"
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            logger.info("Stream load test_variant_high_precision_decimal_stream.json to variant result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberLoadedRows as int)
        }
    }

    sql "sync"

    def assertAllRowsMatch = { String predicate ->
        def result = sql "SELECT count(*) FROM test_variant_high_precision_decimal WHERE ${predicate}"
        assertEquals(3, result[0][0] as int)
    }

    assertAllRowsMatch("cast(v['number_1'] as decimalv3(38,10)) = cast('999999999999999999999999999.9999999990' as decimalv3(38,10))")
    assertAllRowsMatch("cast(v['number_2'] as decimalv3(38,10)) = cast('0.0000000001' as decimalv3(38,10))")
    assertAllRowsMatch("cast(v['number_3'] as decimalv3(38,10)) = cast('-999999999999999999999999999.9999999990' as decimalv3(38,10))")
    assertAllRowsMatch("cast(v['number_4'] as decimalv3(38,18)) = cast('0.570000000000000000' as decimalv3(38,18))")
    assertAllRowsMatch("cast(v['number_big_integer'] as decimalv3(76,0)) = cast('${decimal256Integer}' as decimalv3(76,0))")
    assertAllRowsMatch("cast(v['number_scientific_1'] as decimalv3(38,10)) = cast('999999999999999999999999999.9999999990' as decimalv3(38,10))")
    assertAllRowsMatch("cast(v['number_scientific_2'] as decimalv3(38,10)) = cast('0.0000000001' as decimalv3(38,10))")
    assertAllRowsMatch("cast(v['glob_decimal_1'] as decimalv3(38,10)) = cast('999999999999999999999999999.9999999990' as decimalv3(38,10))")
    assertAllRowsMatch("array_contains(cast(v['numberArray_1'] as array<decimalv3(38,10)>), cast('999999999999999999999999999.9999999990' as decimalv3(38,10)))")
    assertAllRowsMatch("array_contains(cast(v['numberArray_1'] as array<decimalv3(38,10)>), cast('-999999999999999999999999999.9999999990' as decimalv3(38,10)))")

    def stringFirstTypeCount = sql """ SELECT count(*)
                                       FROM test_variant_high_precision_decimal
                                       WHERE cast(variant_type(v['string_first_decimal']) as string) = '{"":"string"}'
                                         AND cast(v['string_first_decimal'] as string) != '999999999999999999999999999.999999999' """
    assertEquals(3, stringFirstTypeCount[0][0] as int)

    def untypedTypeCount = sql """ SELECT count(*)
                                   FROM test_variant_high_precision_decimal
                                   WHERE cast(variant_type(v['untyped_decimal']) as string) = '{"":"double"}' """
    assertEquals(3, untypedTypeCount[0][0] as int)

    def untypedBigIntegerTypeCount = sql """ SELECT count(*)
                                             FROM test_variant_high_precision_decimal
                                             WHERE cast(variant_type(v['untyped_big_integer']) as string) = '{"":"double"}' """
    assertEquals(3, untypedBigIntegerTypeCount[0][0] as int)
}
