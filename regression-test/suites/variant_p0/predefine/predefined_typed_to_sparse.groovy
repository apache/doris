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
suite("test_predefine_typed_to_sparse", "p0"){ 
    sql """ set enable_common_expr_pushdown = true """
    def count = new Random().nextInt(10) + 1

     def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'max_filter_ratio', '0.1'
            set 'memtable_on_sink_node', 'true'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def tableName = "test_predefine_typed_to_sparse"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL,
        `var`  variant <
                'array_decimal_*':array<decimalv3 (26,9)>,
                'array_ipv6_*':array<ipv6>,
                'int_*':int, 
                'string_*':string, 
                'decimal_*':decimalv3(26,9), 
                'datetime_*':datetime,
                'datetimev2_*':datetimev2(6),
                'date_*':date,
                'datev2_*':datev2,
                'ipv4_*':ipv4,
                'ipv6_*':ipv6,
                'largeint_*':largeint,
                'char_*': text,
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "${count}"
                )
            > NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="array_decimal_*") COMMENT '',
        INDEX idx_a_c (var) USING INVERTED PROPERTIES("field_pattern"="array_ipv6_*") COMMENT '',
        INDEX idx_a_d (var) USING INVERTED PROPERTIES("field_pattern"="int_*") COMMENT '',
        INDEX idx_a_e (var) USING INVERTED PROPERTIES("field_pattern"="string_*", "parser"="english", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_f (var) USING INVERTED PROPERTIES("field_pattern"="decimal_*") COMMENT '',
        INDEX idx_a_g (var) USING INVERTED PROPERTIES("field_pattern"="datetime_*") COMMENT '',
        INDEX idx_a_h (var) USING INVERTED PROPERTIES("field_pattern"="datetimev2_*") COMMENT '',
        INDEX idx_a_i (var) USING INVERTED PROPERTIES("field_pattern"="date_*") COMMENT '',
        INDEX idx_a_j (var) USING INVERTED PROPERTIES("field_pattern"="datev2_*") COMMENT '',
        INDEX idx_a_k (var) USING INVERTED PROPERTIES("field_pattern"="ipv4_*") COMMENT '',
        INDEX idx_a_l (var) USING INVERTED PROPERTIES("field_pattern"="ipv6_*") COMMENT '',
        INDEX idx_a_m (var) USING INVERTED PROPERTIES("field_pattern"="largeint_*") COMMENT '',
        INDEX idx_a_n (var) USING INVERTED PROPERTIES("field_pattern"="char_*") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """

    sql """
         INSERT INTO ${tableName} (`id`, `var`) VALUES
        (
            1,
            '{
              "array_decimal_1": ["12345678901234567.123456789", "987.654321"],
              "array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "::1"],
              "int_1": 42,
              "int_nested": {
                "level1_num_1": 1011111,
                "level1_num_2": 102
              },
              "string_1": "This is a sample string.",
              "string_1_nested": {
                "message": "Hello from nested object",
                "metadata": {
                  "timestamp": "2023-10-27T12:00:00Z",
                  "source": "generator"
                }
              },
              "decimal_1": 12345.6789,
              "datetime_1": "2023-10-27 10:30:00",
              "datetimev2_1": "2023-10-27 10:30:00.123456",
              "date_1": "2023-10-27",
              "datev2_1": "2023-10-28",
              "ipv4_1": "192.168.1.1",
              "ipv6_1": "::1",
              "largeint_1": "12345678901234567890123456789012345678",
              "char_1": "short text"
            }'
        ); 
    """
    for (int i = 1; i < 10; i++) {
      load_json_data.call(tableName, getS3Url() + "/regression/variant/schema_tmpt${i}.json")
    }

    qt_sql """ select * from ${tableName} order by id limit 10 """

    trigger_and_wait_compaction(tableName, "cumulative")

    qt_sql """ select * from ${tableName} order by id limit 10"""

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL,
        `var`  variant <
                'array_decimal_*':array<decimalv3 (26,9)>,
                'array_ipv6_*':array<ipv6>,
                'int_*':int, 
                'string_*':string, 
                'decimal_*':decimalv3(26,9), 
                'datetime_*':datetime,
                'datetimev2_*':datetimev2(6),
                'date_*':date,
                'datev2_*':datev2,
                'ipv4_*':ipv4,
                'ipv6_*':ipv6,
                'largeint_*':largeint,
                'char_*': text,
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "5"
                )
            > NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="array_decimal_*") COMMENT '',
        INDEX idx_a_c (var) USING INVERTED PROPERTIES("field_pattern"="array_ipv6_*") COMMENT '',
        INDEX idx_a_d (var) USING INVERTED PROPERTIES("field_pattern"="int_*") COMMENT '',
        INDEX idx_a_e (var) USING INVERTED PROPERTIES("field_pattern"="string_*", "parser"="english", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_f (var) USING INVERTED PROPERTIES("field_pattern"="decimal_*") COMMENT '',
        INDEX idx_a_g (var) USING INVERTED PROPERTIES("field_pattern"="datetime_*") COMMENT '',
        INDEX idx_a_h (var) USING INVERTED PROPERTIES("field_pattern"="datetimev2_*") COMMENT '',
        INDEX idx_a_i (var) USING INVERTED PROPERTIES("field_pattern"="date_*") COMMENT '',
        INDEX idx_a_j (var) USING INVERTED PROPERTIES("field_pattern"="datev2_*") COMMENT '',
        INDEX idx_a_k (var) USING INVERTED PROPERTIES("field_pattern"="ipv4_*") COMMENT '',
        INDEX idx_a_l (var) USING INVERTED PROPERTIES("field_pattern"="ipv6_*") COMMENT '',
        INDEX idx_a_m (var) USING INVERTED PROPERTIES("field_pattern"="largeint_*") COMMENT '',
        INDEX idx_a_n (var) USING INVERTED PROPERTIES("field_pattern"="char_*") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")
    """    

    for (int i = 1; i < 10; i++) {
      sql """
          INSERT INTO ${tableName} (`id`, `var`) VALUES
          (
            ${i},
              '{
                "array_decimal_1": ["12345678901234567.123456789", "987.654321"],
                "array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334", "::1"],
                "int_1": 42,
                "int_nested": {
                  "level1_num_1": 1011111,
                  "level1_num_2": 102
                },
                "string_1": "This is a sample string.",
                "string_1_nested": {
                  "message": "Hello from nested object",
                  "metadata": {
                    "timestamp": "2023-10-27T12:00:00Z",
                    "source": "generator"
                  }
                },
                "decimal_1": 12345.6789,
                "datetime_1": "2023-10-27 10:30:00",
                "datetimev2_1": "2023-10-27 10:30:00.123456",
                "date_1": "2023-10-27",
                "datev2_1": "2023-10-28",
                "ipv4_1": "192.168.1.1",
                "ipv6_1": "::1",
                "largeint_1": "12345678901234567890123456789012345678",
                "char_1": "short text"
              }'
          ); 
      """
    }

    qt_sql """ select variant_type(var) from ${tableName} limit 1"""
    qt_sql """ select * from ${tableName} order by id limit 10 """
    qt_sql """ select var['array_decimal_1'], var['array_ipv6_1'], var['int_1'], var['int_nested'], var['string_1'], var['string_1_nested'], var['decimal_1'], var['datetime_1'], var['datetimev2_1'], var['date_1'], var['datev2_1'], var['ipv4_1'], var['ipv6_1'], var['largeint_1'], var['char_1'] from ${tableName} order by id """

    trigger_and_wait_compaction(tableName, "cumulative")

    qt_sql """ select variant_type(var) from ${tableName} limit 1"""
    qt_sql """ select * from ${tableName} order by id limit 10"""
    qt_sql """ select var['array_decimal_1'], var['array_ipv6_1'], var['int_1'], var['int_nested'], var['string_1'], var['string_1_nested'], var['decimal_1'], var['datetime_1'], var['datetimev2_1'], var['date_1'], var['datev2_1'], var['ipv4_1'], var['ipv6_1'], var['largeint_1'], var['char_1'] from ${tableName} order by id """

}