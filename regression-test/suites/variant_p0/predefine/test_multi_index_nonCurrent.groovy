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

suite("test_variant_multi_index_nonCurrent", "p0, nonConcurrent") { 
    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """

    def queryAndCheck = { String sqlQuery, int expectedFilteredRows = -1, boolean checkFilterUsed = true ->
      def checkpoints_name = "segment_iterator.inverted_index.filtered_rows"
      try {
          GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
          GetDebugPoint().enableDebugPointForAllBEs(checkpoints_name, [filtered_rows: expectedFilteredRows])
          sql "set experimental_enable_parallel_scan = false"
          sql "sync"
          sql "${sqlQuery}"
      } finally {
          GetDebugPoint().disableDebugPointForAllBEs(checkpoints_name)
          GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
      }
    }

    def tableName = "test_variant_multi_index_nonCurrent"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant NOT NULL,
        INDEX idx_a_d (var) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_d_2 (var) USING INVERTED
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "variant_max_subcolumns_count" = "10")"""

    sql """insert into ${tableName} values(1, '{"string" : "hello", "array_string" : ["hello"]}'), (2, '{"string" : "world", "array_string" : ["world"]}'), (3, '{"string" : "hello", "array_string" : ["hello"]}'), (4, '{"string" : "world", "array_string" : ["world"]}'), (5, '{"string" : "hello", "array_string" : ["hello"]}') """
    // insert into test_variant_multi_index_nonCurrent  values(1, '{"string" : "hello", "array_string" : ["hello"]}'), (2, '{"string" : "world", "array_string" : ["world"]}'), (3, '{"string" : "hello", "array_string" : ["hello"]}'), (4, '{"string" : "world", "array_string" : ["world"]}'), (5, '{"string" : "hello", "array_string" : ["hello"]}')
    sql """ set inverted_index_skip_threshold = 0 """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """
    
    queryAndCheck("select count() from ${tableName} where var['string'] match_phrase 'hello'", 2)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello'", 2)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello', 'world')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello')", 2)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello')", 3)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello', 'world')", 5)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('helloworld')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('helloworld')", 5)
    queryAndCheck("select count() from ${tableName} where var['string'] != 'world'", 2)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello' or var['string'] match_phrase 'world'", 0)
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_string'] as array<text>), 'hello')", 2)

    for (int i = 0; i < 10; i++) {
        sql """insert into ${tableName} values(1, '{"string" : "hello", "array_string" : ["hello"]}'), (2, '{"string" : "world", "array_string" : ["world"]}'), (3, '{"string" : "hello", "array_string" : ["hello"]}'), (4, '{"string" : "world", "array_string" : ["world"]}'), (5, '{"string" : "hello", "array_string" : ["hello"]}') """
    }
    trigger_and_wait_compaction(tableName, "cumulative")

    queryAndCheck("select count() from ${tableName} where var['string'] match_phrase 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello', 'world')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello')", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello')", 33)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello', 'world')", 55)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('helloworld')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('helloworld')", 55)
    queryAndCheck("select count() from ${tableName} where var['string'] != 'world'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello' or var['string'] match_phrase 'world'", 0)
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_string'] as array<text>), 'hello')", 22)

    sql """ alter table ${tableName} modify column var variant NULL """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    queryAndCheck("select count() from ${tableName} where var['string'] match_phrase 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello' or var['string'] match 'world'", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello', 'world')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('hello')", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello')", 33)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('hello', 'world')", 55)
    queryAndCheck("select count() from ${tableName} where var['string'] not in ('helloworld')", 0)
    queryAndCheck("select count() from ${tableName} where var['string'] in ('helloworld')", 55)
    queryAndCheck("select count() from ${tableName} where var['string'] != 'world'", 22)
    queryAndCheck("select count() from ${tableName} where var['string'] = 'hello' or var['string'] match_phrase 'world'", 0)
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_string'] as array<text>), 'hello')", 22)


    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
             MATCH_NAME 'string1' : string,
             MATCH_NAME 'string2' : string,
             MATCH_NAME 'array_string' : array<string>
        > NOT NULL,
        INDEX idx_a_d_2 (var) USING INVERTED,
        INDEX idx_a_d_3 (var) USING INVERTED PROPERTIES("field_pattern" = "string1","parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_d_4 (var) USING INVERTED PROPERTIES("field_pattern" = "string1") COMMENT '',
        INDEX idx_a_d_5 (var) USING INVERTED PROPERTIES("field_pattern" = "string2","parser"="unicode", "support_phrase" = "false") COMMENT '',
        INDEX idx_a_d_6 (var) USING INVERTED PROPERTIES("field_pattern" = "string2") COMMENT '',
        INDEX idx_a_d_7 (var) USING INVERTED PROPERTIES("field_pattern" = "array_string") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "variant_max_subcolumns_count" = "10")"""

    sql """insert into ${tableName} values(1, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}'),
                                (2, '{"string1" : "world", "array_string" : ["world"], "string2" : "world"}'),
                                (3, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}'),
                                (4, '{"string1" : "world", "array_string" : ["world"], "string2" : "world"}'),
                                (5, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}') """

    sql """ set inverted_index_skip_threshold = 0 """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_match_without_inverted_index = false """
    
    queryAndCheck("select count() from ${tableName} where var['string1'] match_phrase 'hello'", 2)
    queryAndCheck("select count() from ${tableName} where var['string1'] = 'hello'", 2)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('hello', 'world')", 0)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('hello')", 2)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('hello')", 3)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('hello', 'world')", 5)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('helloworld')", 0)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('helloworld')", 5)
    queryAndCheck("select count() from ${tableName} where var['string1'] != 'world'", 2)
    queryAndCheck("select count() from ${tableName} where var['string1'] = 'hello' or var['string1'] match_phrase 'world'", 0)
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_string'] as array<text>), 'hello')", 2)

    boolean findException = false
    try {
        sql """ select count() from ${tableName} where var['string2'] match_phrase 'world' """
    } catch (Exception e) {
        logger.info(e.getMessage())
        assert(e.getMessage().contains("phrase queries require setting support_phrase = true"))
        findException = true
    }
    assertTrue(findException)
    queryAndCheck("select count() from ${tableName} where var['string2'] = 'world'", 3)

    for (int i = 0; i < 10; i++) {
        sql """insert into ${tableName} values(1, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}'),
                                            (2, '{"string1" : "world", "array_string" : ["world"], "string2" : "world"}'),
                                            (3, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}'),
                                            (4, '{"string1" : "world", "array_string" : ["world"], "string2" : "world"}'),
                                            (5, '{"string1" : "hello", "array_string" : ["hello"], "string2" : "hello"}') """
    }
    trigger_and_wait_compaction(tableName, "cumulative")

    queryAndCheck("select count() from ${tableName} where var['string1'] match_phrase 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string1'] = 'hello'", 22)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('hello', 'world')", 0)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('hello')", 22)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('hello')", 33)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('hello', 'world')", 55)
    queryAndCheck("select count() from ${tableName} where var['string1'] not in ('helloworld')", 0)
    queryAndCheck("select count() from ${tableName} where var['string1'] in ('helloworld')", 55)
    queryAndCheck("select count() from ${tableName} where var['string1'] != 'world'", 22)
    queryAndCheck("select count() from ${tableName} where var['string1'] = 'hello' or var['string1'] match_phrase 'world'", 0)
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_string'] as array<text>), 'hello')", 22)
    findException = false
    try {
        sql """ select count() from ${tableName} where var['string2'] match_phrase 'world' """
    } catch (Exception e) {
        logger.info(e.getMessage())
        assert(e.getMessage().contains("phrase queries require setting support_phrase = true"))
        findException = true
    }
    assertTrue(findException)
    queryAndCheck("select count() from ${tableName} where var['string2'] = 'world'", 33)


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

    tableName = "test_variant_predefine_types_with_multi_indexes"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL AUTO_INCREMENT,
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
                'char_*':text
            > NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="array_decimal_*") COMMENT '',
        INDEX idx_a_c (var) USING INVERTED PROPERTIES("field_pattern"="array_ipv6_*") COMMENT '',
        INDEX idx_a_d (var) USING INVERTED PROPERTIES("field_pattern"="int_*") COMMENT '',
        INDEX idx_a_e (var) USING INVERTED PROPERTIES("field_pattern"="string_*", "parser"="english", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_e_2 (var) USING INVERTED PROPERTIES("field_pattern"="string_*") COMMENT '',
        INDEX idx_a_f (var) USING INVERTED PROPERTIES("field_pattern"="decimal_*") COMMENT '',
        INDEX idx_a_g (var) USING INVERTED PROPERTIES("field_pattern"="datetime_*") COMMENT '',
        INDEX idx_a_h (var) USING INVERTED PROPERTIES("field_pattern"="datetimev2_*") COMMENT '',
        INDEX idx_a_i (var) USING INVERTED PROPERTIES("field_pattern"="date_*") COMMENT '',
        INDEX idx_a_j (var) USING INVERTED PROPERTIES("field_pattern"="datev2_*") COMMENT '',
        INDEX idx_a_k (var) USING INVERTED PROPERTIES("field_pattern"="ipv4_*") COMMENT '',
        INDEX idx_a_l (var) USING INVERTED PROPERTIES("field_pattern"="ipv6_*") COMMENT '',
        INDEX idx_a_m (var) USING INVERTED PROPERTIES("field_pattern"="largeint_*") COMMENT '',
        INDEX idx_a_n (var) USING INVERTED PROPERTIES("field_pattern"="char_*") COMMENT '',
        INDEX idx_a_o (var) USING INVERTED PROPERTIES("field_pattern"="char_*", "parser"="english", "support_phrase" = "false") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """
    sql """
         INSERT INTO ${tableName} (`var`) VALUES
        (
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

    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_decimal_1'] as array<decimalv3 (26,9)>), 12345678901234567.123456789)", 0)
    //qt_sql "select count() from ${tableName} where array_contains(cast(var['array_decimal_1'] as array<decimalv3 (26,9)>), 12345678901234567.123456689)"
    queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_decimal_1'] as array<decimalv3 (26,9)>), 12345678901234567.123456689)", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['int_1'] as int) = 42", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['int_1'] as int) = 43", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['int_nested.level1_num_1'] as int) = 1011111", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['int_nested.level1_num_1'] as int) = 1011112", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['int_nested']['level1_num_1'] as int) = 1011111", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['int_nested']['level1_num_1'] as int) = 1011112", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1'] match 'sample'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1'] match 'samples'", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match 'Hello'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match 'Hellos'", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_all 'nested object'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_all 'nested objects'", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_any 'object'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_any 'objects'", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] match_all '2023-10-27T12:00:00Z'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] match_all '2023-10-28T12:00:00Z'", 1)

    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] = '2023-10-27T12:00:00Z'", 0)
    queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] = '2023-10-27T12:00:00S'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['decimal_1'] as decimalv3(26,9)) = 12345.6789", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['decimal_1'] as decimalv3(26,9)) = 12345.67819", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['datetime_1'] as datetime) = '2023-10-27 10:30:00'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['datetime_1'] as datetime) = '2023-10-27 10:30:01'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['datetimev2_1'] as datetimev2(6)) = '2023-10-27 10:30:00.123456'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['datetimev2_1'] as datetimev2(6)) = '2023-10-27 10:30:01.123456'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['date_1'] as date) = '2023-10-27'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['date_1'] as date) = '2023-10-28'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['datev2_1'] as datev2) = '2023-10-28'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['datev2_1'] as datev2) = '2023-10-29'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['ipv4_1'] as ipv4) = '192.168.1.1'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['ipv4_1'] as ipv4) = '192.168.1.2'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['ipv6_1'] as ipv6) = '::1'", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['ipv6_1'] as ipv6) = '::2'", 1)

    queryAndCheck("select count() from ${tableName} where cast(var['largeint_1'] as largeint) = 12345678901234567890123456789012345678", 0)
    queryAndCheck("select count() from ${tableName} where cast(var['largeint_1'] as largeint) = 1234567890123456789123456789012345679", 1)

    queryAndCheck("select count() from ${tableName} where var['char_1'] = 'short text'", 0)
    queryAndCheck("select count() from ${tableName} where var['char_1'] = 'short texts'", 1)

    queryAndCheck("select count() from ${tableName} where var['char_1'] match_all 'short text'", 0)
    queryAndCheck("select count() from ${tableName} where var['char_1'] match_all 'short texts'", 1)
    
    findException = false
    try {
        sql """ select count() from ${tableName} where var['char_1'] match_phrase 'short text' """
    } catch (Exception e) {
        logger.info(e.getMessage())
        assert(e.getMessage().contains("phrase queries require setting support_phrase = true"))
        findException = true
    }
    assertTrue(findException)

    for (int i = 1; i < 10; i++) {
      load_json_data.call(tableName, getS3Url() + "/regression/variant/schema_tmpt${i}.json")
    }

    def accurateCheckIndexWithQueries = { ->
      queryAndCheck("select count() from ${tableName} where array_contains(cast(var['array_decimal_1'] as array<decimalv3 (26,9)>), 12345678901234567.123456789)", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['int_1'] as int) = 42", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['int_nested.level1_num_1'] as int) = 1011111", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['int_nested']['level1_num_1'] as int) = 1011111", 90000)

      queryAndCheck("select count() from ${tableName} where var['string_1'] match 'sample'", 82222)

      queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match 'Hello'", 90000)

      queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_all 'nested object'", 88730)

      queryAndCheck("select count() from ${tableName} where var['string_1_nested']['message'] match_any 'object'", 82173)

      queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] match '2023-10-27T12:00:00Z'", 90000)

      queryAndCheck("select count() from ${tableName} where var['string_1_nested']['metadata']['timestamp'] match '2023-10-27T12:00:00Z'", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['decimal_1'] as decimalv3(26,9)) = 12345.6789", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['datetime_1'] as datetime) = '2023-10-27 10:30:00'", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['datetimev2_1'] as datetimev2(6)) = '2023-10-27 10:30:00.123456'", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['date_1'] as date) = '2023-10-27'", 89976)

      queryAndCheck("select count() from ${tableName} where cast(var['datev2_1'] as datev2) = '2023-10-28'", 89974)

      queryAndCheck("select count() from ${tableName} where cast(var['ipv4_1'] as ipv4) = '192.168.1.1'", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['ipv6_1'] as ipv6) = '::1'", 90000)

      queryAndCheck("select count() from ${tableName} where cast(var['largeint_1'] as largeint) = 12345678901234567890123456789012345678", 90000)
    }

    sql "set enable_two_phase_read_opt = false"
    qt_sql "select * from ${tableName} order by id limit 10"
    trigger_and_wait_compaction(tableName, "cumulative")
    sql "set enable_two_phase_read_opt = true"
    qt_sql "select * from ${tableName} order by id limit 10"
    qt_sql "select variant_type(var) from ${tableName} where id = 1"
    accurateCheckIndexWithQueries()
    findException = false
    try {
        sql """ select count() from ${tableName} where var['char_1'] match_phrase 'short text' """
    } catch (Exception e) {
        logger.info(e.getMessage())
        assert(e.getMessage().contains("phrase queries require setting support_phrase = true"))
        findException = true
    }
    assertTrue(findException)
}