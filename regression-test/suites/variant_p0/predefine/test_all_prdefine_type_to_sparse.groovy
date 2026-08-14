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

suite("test_all_prdefine_type_to_sparse", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: false]) {
    assertFalse(getFeConfig("enable_variant_v2").toBoolean())
    def variantV2Function = ""

    sql """ set describe_extend_variant_column = true """
    sql """ set default_variant_enable_doc_mode = false """

    def tableName = "test_all_prdefine_type_to_sparse"
    sql "set enable_decimal256 = true"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL,
        `var`  variant <
                'boolean_*':boolean,
                'tinyint_*':tinyint,
                'smallint_*':smallint,
                'int_*':int,
                'bigint_*':bigint,
                'largeint_*':largeint,
                'char_*': text,
                'string_*':string,
                'float_*':float,
                'double_*':double,
                'decimal32_*':decimalv3(8,2),
                'decimal64_*':decimalv3(16,9),
                'decimal128_*':decimalv3(36,9),
                'decimal256_*':decimalv3(70,60),
                'datetime_*':datetime,
                'date_*':date,
                'ipv4_*':ipv4,
                'ipv6_*':ipv6,
                'array_boolean_*':array<boolean>,
                'array_tinyint_*':array<tinyint>,
                'array_smallint_*':array<smallint>,
                'array_int_*':array<int>,
                'array_bigint_*':array<bigint>,
                'array_largeint_*':array<largeint>,
                'array_char_*':array<text>,
                'array_string_*':array<string>,
                'array_float_*':array<float>,
                'array_double_*':array<double>,
                'array_decimal32_*':array<decimalv3(8,2)>,
                'array_decimal64_*':array<decimalv3(16,9)>,
                'array_decimal128_*':array<decimalv3(36,9)>,
                'array_decimal256_*':array<decimalv3(70,60)>,
                'array_datetime_*':array<datetime>,
                'array_date_*':array<date>,
                'array_ipv4_*':array<ipv4>,
                'array_ipv6_*':array<ipv6>,
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "1",
                    "variant_sparse_hash_shard_count" = "3"
                )
            > NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """

    sql """
         INSERT INTO ${tableName} VALUES
        (0,
            ${variantV2Function}('{
              "boolean_1": true,
              "tinyint_1": 1,
              "smallint_1": 1,
              "int_1": 1,
              "bigint_1": 1,
              "largeint_1": 1,
              "char_1": "1",
              "string_1": "1",
              "float_1": 1.12,
              "double_1": 1.12,
              "decimal32_1": 1.12,
              "decimal64_1": 1.12,
              "decimal128_1": 1.12,
              "decimal256_1": 1.12,
              "datetime_1": "2021-01-01 00:00:00",
              "date_1": "2021-01-01",
              "ipv4_1": "192.168.1.1",
              "ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
              "array_boolean_1": [true],
              "array_tinyint_1": [1, null],
              "array_smallint_1": [1, null],
              "array_int_1": [1, null],
              "array_bigint_1": [1, null],
              "array_largeint_1": [1, null],
              "array_char_1": ["1"],
              "array_string_1": ["1"],
              "array_float_1": [1.12],
              "array_double_1": [1.12],
              "array_decimal32_1": [1.12],
              "array_decimal64_1": [1.12],
              "array_decimal128_1": [1.12],
              "array_decimal256_1": [1.12],
              "array_datetime_1": ["2021-01-01 00:00:00"],
              "array_date_1": ["2021-01-01"],
              "array_ipv4_1": ["192.168.1.1"],
              "array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],
              "other_1": "1"
            }')
        ),
        (1,
            ${variantV2Function}('{"other_1": "1"}')
        );
    """

    qt_sql """ select variant_type(var) from ${tableName} limit 1"""
    qt_sql """ select var from ${tableName} order by id """


    def check_table = {
        def before_result = sql """ select var from ${tableName} order by id """
        log.info("before_result: ${before_result}")
        qt_sql_compaction_before """ desc ${tableName} """

        trigger_and_wait_compaction(tableName, "full", 1800)

        def after_result = sql """ select var from ${tableName} order by id """
        log.info("after_result: ${after_result}")
        assertTrue(before_result.toString() == after_result.toString())

        qt_sql_compaction_after """ desc ${tableName} """
        qt_sql """ select var from ${tableName} order by id """
    }

    sql """ insert into ${tableName} values (2, ${variantV2Function}('{"tinyint_1": 1}')),(3, ${variantV2Function}('{"tinyint_1": 2}')); """

    check_table();

    sql """ insert into ${tableName} values (4, ${variantV2Function}('{"smallint_1": 1}')),(5, ${variantV2Function}('{"smallint_1": 2}')),(6, ${variantV2Function}('{"smallint_1": 3}')); """

    check_table();

    sql """ insert into ${tableName}  values (7, ${variantV2Function}('{"int_1": 1}')),(8, ${variantV2Function}('{"int_1": 2}')),(9, ${variantV2Function}('{"int_1": 3}')),(10, ${variantV2Function}('{"int_1": 4}')); """

    check_table();

    sql """ insert into ${tableName}  values (11, ${variantV2Function}('{"bigint_1": 1}')),(12, ${variantV2Function}('{"bigint_1": 2}')),(13, ${variantV2Function}('{"bigint_1": 3}')),(14, ${variantV2Function}('{"bigint_1": 4}')),(15, ${variantV2Function}('{"bigint_1": 5}')); """

    check_table();

    sql """ insert into ${tableName}  values (16, ${variantV2Function}('{"largeint_1": 1}')),(17, ${variantV2Function}('{"largeint_1": 2}')),(18, ${variantV2Function}('{"largeint_1": 3}')),(19, ${variantV2Function}('{"largeint_1": 4}')),(20, ${variantV2Function}('{"largeint_1": 5}')),(21, ${variantV2Function}('{"largeint_1": 6}')); """

    check_table();

    sql """ insert into ${tableName}  values (22, ${variantV2Function}('{"char_1": "1"}')),(23, ${variantV2Function}('{"char_1": "2"}')),(24, ${variantV2Function}('{"char_1": "3"}')),(25, ${variantV2Function}('{"char_1": "4"}')),(26, ${variantV2Function}('{"char_1": "5"}')),(27, ${variantV2Function}('{"char_1": "6"}')),(28, ${variantV2Function}('{"char_1": "7"}')); """

    check_table();

    sql """ insert into ${tableName}  values (29, ${variantV2Function}('{"string_1": "1"}')),(30, ${variantV2Function}('{"string_1": "2"}')),(31, ${variantV2Function}('{"string_1": "3"}')),(32, ${variantV2Function}('{"string_1": "4"}')),(33, ${variantV2Function}('{"string_1": "5"}')),
    (34, ${variantV2Function}('{"string_1": "6"}')),(35, ${variantV2Function}('{"string_1": "7"}')),(36, ${variantV2Function}('{"string_1": "8"}')); """

    check_table();

    sql """ insert into ${tableName}  values (37, ${variantV2Function}('{"float_1": 1.12}')),(38, ${variantV2Function}('{"float_1": 2.12}')),(39, ${variantV2Function}('{"float_1": 3.12}')),(40, ${variantV2Function}('{"float_1": 4.12}')),(41, ${variantV2Function}('{"float_1": 5.12}')),
    (42, ${variantV2Function}('{"float_1": 6.12}')),(43, ${variantV2Function}('{"float_1": 7.12}')),(44, ${variantV2Function}('{"float_1": 8.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (45, ${variantV2Function}('{"double_1": 1.12}')),(46, ${variantV2Function}('{"double_1": 2.12}')),(47, ${variantV2Function}('{"double_1": 3.12}')),(48, ${variantV2Function}('{"double_1": 4.12}')),(49, ${variantV2Function}('{"double_1": 5.12}')),
    (50, ${variantV2Function}('{"double_1": 6.12}')),(51, ${variantV2Function}('{"double_1": 7.12}')),(52, ${variantV2Function}('{"double_1": 8.12}')),(53, ${variantV2Function}('{"double_1": 9.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (54, ${variantV2Function}('{"decimal32_1": 1.12}')),(55, ${variantV2Function}('{"decimal32_1": 2.12}')),(56, ${variantV2Function}('{"decimal32_1": 3.12}')),(57, ${variantV2Function}('{"decimal32_1": 4.12}')),(58, ${variantV2Function}('{"decimal32_1": 5.12}')),
    (59, ${variantV2Function}('{"decimal32_1": 6.12}')),(60, ${variantV2Function}('{"decimal32_1": 7.12}')),(61, ${variantV2Function}('{"decimal32_1": 8.12}')),(62, ${variantV2Function}('{"decimal32_1": 9.12}')),(63, ${variantV2Function}('{"decimal32_1": 10.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (64, ${variantV2Function}('{"decimal64_1": 1.12}')),(65, ${variantV2Function}('{"decimal64_1": 2.12}')),(66, ${variantV2Function}('{"decimal64_1": 3.12}')),(67, ${variantV2Function}('{"decimal64_1": 4.12}')),(68, ${variantV2Function}('{"decimal64_1": 5.12}')),
    (69, ${variantV2Function}('{"decimal64_1": 6.12}')),(70, ${variantV2Function}('{"decimal64_1": 7.12}')),(71, ${variantV2Function}('{"decimal64_1": 8.12}')),(72, ${variantV2Function}('{"decimal64_1": 9.12}')),(73, ${variantV2Function}('{"decimal64_1": 10.12}')),(74, ${variantV2Function}('{"decimal64_1": 11.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (75, ${variantV2Function}('{"decimal128_1": 1.12}')),(76, ${variantV2Function}('{"decimal128_1": 2.12}')),(77, ${variantV2Function}('{"decimal128_1": 3.12}')),(78, ${variantV2Function}('{"decimal128_1": 4.12}')),(79, ${variantV2Function}('{"decimal128_1": 5.12}')),
    (80, ${variantV2Function}('{"decimal128_1": 6.12}')),(81, ${variantV2Function}('{"decimal128_1": 7.12}')),(82, ${variantV2Function}('{"decimal128_1": 8.12}')),(83, ${variantV2Function}('{"decimal128_1": 9.12}')),(84, ${variantV2Function}('{"decimal128_1": 10.12}')),(85, ${variantV2Function}('{"decimal128_1": 11.12}')),
    (86, ${variantV2Function}('{"decimal128_1": 12.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (87, ${variantV2Function}('{"decimal256_1": 1.12}')),(88, ${variantV2Function}('{"decimal256_1": 2.12}')),(89, ${variantV2Function}('{"decimal256_1": 3.12}')),(90, ${variantV2Function}('{"decimal256_1": 4.12}')),(91, ${variantV2Function}('{"decimal256_1": 5.12}')),
    (92, ${variantV2Function}('{"decimal256_1": 6.12}')),(93, ${variantV2Function}('{"decimal256_1": 7.12}')),(94, ${variantV2Function}('{"decimal256_1": 8.12}')),(95, ${variantV2Function}('{"decimal256_1": 9.12}')),(96, ${variantV2Function}('{"decimal256_1": 10.12}')),(97, ${variantV2Function}('{"decimal256_1": 11.12}')),
    (98, ${variantV2Function}('{"decimal256_1": 12.12}')),(99, ${variantV2Function}('{"decimal256_1": 13.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (100, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:00"}')),(101, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:01"}')),(102, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:02"}')),
    (103, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:03"}')),(104, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:04"}')),(105, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:05"}')),(106, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:06"}')),
    (107, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:07"}')),(108, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:08"}')),(109, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:09"}')),(110, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:10"}')),
    (111, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:07"}')),(112, ${variantV2Function}('{"datetime_1": "2021-01-01 00:00:08"}')); """

    check_table();

    sql """ insert into ${tableName}  values (113, ${variantV2Function}('{"date_1": "2021-01-01"}')),(114, ${variantV2Function}('{"date_1": "2021-01-02"}')),(115, ${variantV2Function}('{"date_1": "2021-01-03"}')),(116, ${variantV2Function}('{"date_1": "2021-01-04"}')),
    (117, ${variantV2Function}('{"date_1": "2021-01-05"}')),(118, ${variantV2Function}('{"date_1": "2021-01-06"}')),(119, ${variantV2Function}('{"date_1": "2021-01-07"}')),(120, ${variantV2Function}('{"date_1": "2021-01-08"}')),(121, ${variantV2Function}('{"date_1": "2021-01-09"}')),(122, ${variantV2Function}('{"date_1": "2021-01-10"}')),
    (123, ${variantV2Function}('{"date_1": "2021-01-07"}')),(124, ${variantV2Function}('{"date_1": "2021-01-08"}')),(125, ${variantV2Function}('{"date_1": "2021-01-09"}')),(126, ${variantV2Function}('{"date_1": "2021-01-10"}')); """

    check_table();

    sql """ insert into ${tableName}  values (127, ${variantV2Function}('{"ipv4_1": "192.168.1.1"}')),(128, ${variantV2Function}('{"ipv4_1": "192.168.1.2"}')),(129, ${variantV2Function}('{"ipv4_1": "192.168.1.3"}')),(130, ${variantV2Function}('{"ipv4_1": "192.168.1.4"}')),
    (131, ${variantV2Function}('{"ipv4_1": "192.168.1.5"}')),(132, ${variantV2Function}('{"ipv4_1": "192.168.1.6"}')),(133, ${variantV2Function}('{"ipv4_1": "192.168.1.7"}')),(134, ${variantV2Function}('{"ipv4_1": "192.168.1.8"}')),(135, ${variantV2Function}('{"ipv4_1": "192.168.1.9"}')),(136, ${variantV2Function}('{"ipv4_1": "192.168.1.10"}')),
    (137, ${variantV2Function}('{"ipv4_1": "192.168.1.7"}')),(138, ${variantV2Function}('{"ipv4_1": "192.168.1.8"}')),(139, ${variantV2Function}('{"ipv4_1": "192.168.1.9"}')),(140, ${variantV2Function}('{"ipv4_1": "192.168.1.10"}')),(141, ${variantV2Function}('{"ipv4_1": "192.168.1.11"}')); """

    check_table();

    sql """ insert into ${tableName}  values (142, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}')),(143, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7335"}')),
    (144, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7336"}')),(145, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7337"}')),(146, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7338"}')),
    (147, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7339"}')),(148, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733a"}')),(149, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733b"}')),
    (150, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733c"}')),(151, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733d"}')),(152, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733e"}')),
    (153, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}')),(154, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}')),(155, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7341"}')),
    (156, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}')),(157, ${variantV2Function}('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}')); """

    check_table();

    sql """ insert into ${tableName}  values (158, ${variantV2Function}('{"array_boolean_1": [true]}')),(159, ${variantV2Function}('{"array_boolean_1": [false]}')),(160, ${variantV2Function}('{"array_boolean_1": [true]}')),(161, ${variantV2Function}('{"array_boolean_1": [false]}')),
    (162, ${variantV2Function}('{"array_boolean_1": [true]}')),(163, ${variantV2Function}('{"array_boolean_1": [false]}')),(164, ${variantV2Function}('{"array_boolean_1": [true]}')),(165, ${variantV2Function}('{"array_boolean_1": [false]}')),(166, ${variantV2Function}('{"array_boolean_1": [true]}')),(167, ${variantV2Function}('{"array_boolean_1": [false]}')),
    (168, ${variantV2Function}('{"array_boolean_1": [true]}')),(169, ${variantV2Function}('{"array_boolean_1": [false]}')),(170, ${variantV2Function}('{"array_boolean_1": [true]}')),(171, ${variantV2Function}('{"array_boolean_1": [false]}')),(172, ${variantV2Function}('{"array_boolean_1": [true]}')),(173, ${variantV2Function}('{"array_boolean_1": [false]}'));"""

    check_table();

    sql """ insert into ${tableName}  values (174, ${variantV2Function}('{"array_tinyint_1": [1]}')),(175, ${variantV2Function}('{"array_tinyint_1": [2]}')),(176, ${variantV2Function}('{"array_tinyint_1": [3]}')),(177, ${variantV2Function}('{"array_tinyint_1": [4]}')),
    (178, ${variantV2Function}('{"array_tinyint_1": [5]}')),(179, ${variantV2Function}('{"array_tinyint_1": [6]}')),(180, ${variantV2Function}('{"array_tinyint_1": [7]}')),(181, ${variantV2Function}('{"array_tinyint_1": [8]}')),(182, ${variantV2Function}('{"array_tinyint_1": [9]}')),(183, ${variantV2Function}('{"array_tinyint_1": [10]}')),
    (184, ${variantV2Function}('{"array_tinyint_1": [11]}')),(185, ${variantV2Function}('{"array_tinyint_1": [12]}')),(186, ${variantV2Function}('{"array_tinyint_1": [13]}')),(187, ${variantV2Function}('{"array_tinyint_1": [14]}')),(188, ${variantV2Function}('{"array_tinyint_1": [15]}')),(189, ${variantV2Function}('{"array_tinyint_1": [16]}')),
    (190, ${variantV2Function}('{"array_tinyint_1": [17]}')),(191, ${variantV2Function}('{"array_tinyint_1": [18]}')); """

    check_table();

    sql """ insert into ${tableName}  values (192, ${variantV2Function}('{"array_smallint_1": [1]}')),(193, ${variantV2Function}('{"array_smallint_1": [2, null]}')),(194, ${variantV2Function}('{"array_smallint_1": [3]}')),(195, ${variantV2Function}('{"array_smallint_1": [4]}')),
    (196, ${variantV2Function}('{"array_smallint_1": [5]}')),(197, ${variantV2Function}('{"array_smallint_1": [6]}')),(198, ${variantV2Function}('{"array_smallint_1": [7]}')),(199, ${variantV2Function}('{"array_smallint_1": [8]}')),(200, ${variantV2Function}('{"array_smallint_1": [9]}')),(201, ${variantV2Function}('{"array_smallint_1": [10]}')),
    (202, ${variantV2Function}('{"array_smallint_1": [11]}')),(203, ${variantV2Function}('{"array_smallint_1": [12]}')),(204, ${variantV2Function}('{"array_smallint_1": [13]}')),(205, ${variantV2Function}('{"array_smallint_1": [14]}')),(206, ${variantV2Function}('{"array_smallint_1": [15]}')),(207, ${variantV2Function}('{"array_smallint_1": [16]}')),
    (208, ${variantV2Function}('{"array_smallint_1": [17]}')),(209, ${variantV2Function}('{"array_smallint_1": [18]}')),(210, ${variantV2Function}('{"array_smallint_1": [19]}')); """

    check_table();

    sql """ insert into ${tableName}  values (211, ${variantV2Function}('{"array_int_1": [1]}')),(212, ${variantV2Function}('{"array_int_1": [2]}')),(213, ${variantV2Function}('{"array_int_1": [3]}')),(214, ${variantV2Function}('{"array_int_1": [4]}')),
    (215, ${variantV2Function}('{"array_int_1": [5]}')),(216, ${variantV2Function}('{"array_int_1": [6]}')),(217, ${variantV2Function}('{"array_int_1": [7]}')),(218, ${variantV2Function}('{"array_int_1": [8]}')),(219, ${variantV2Function}('{"array_int_1": [9]}')),(220, ${variantV2Function}('{"array_int_1": [10]}')),
    (221, ${variantV2Function}('{"array_int_1": [11]}')),(222, ${variantV2Function}('{"array_int_1": [12]}')),(223, ${variantV2Function}('{"array_int_1": [13]}')),(224, ${variantV2Function}('{"array_int_1": [14]}')),(225, ${variantV2Function}('{"array_int_1": [15]}')),(226, ${variantV2Function}('{"array_int_1": [16]}')),
    (227, ${variantV2Function}('{"array_int_1": [17]}')),(228, ${variantV2Function}('{"array_int_1": [18]}')),(229, ${variantV2Function}('{"array_int_1": [19]}')),(230, ${variantV2Function}('{"array_int_1": [20]}')); """

    check_table();

    sql """ insert into ${tableName}  values (231, ${variantV2Function}('{"array_bigint_1": [1]}')),(232, ${variantV2Function}('{"array_bigint_1": [2]}')),(233, ${variantV2Function}('{"array_bigint_1": [3]}')),(234, ${variantV2Function}('{"array_bigint_1": [4]}')),
    (235, ${variantV2Function}('{"array_bigint_1": [5]}')),(236, ${variantV2Function}('{"array_bigint_1": [6]}')),(237, ${variantV2Function}('{"array_bigint_1": [7]}')),(238, ${variantV2Function}('{"array_bigint_1": [8]}')),(239, ${variantV2Function}('{"array_bigint_1": [9]}')),(240, ${variantV2Function}('{"array_bigint_1": [10]}')),
    (241, ${variantV2Function}('{"array_bigint_1": [11]}')),(242, ${variantV2Function}('{"array_bigint_1": [12]}')),(243, ${variantV2Function}('{"array_bigint_1": [13]}')),(244, ${variantV2Function}('{"array_bigint_1": [14]}')),(245, ${variantV2Function}('{"array_bigint_1": [15]}')),(246, ${variantV2Function}('{"array_bigint_1": [16]}')),
    (247, ${variantV2Function}('{"array_bigint_1": [17]}')),(248, ${variantV2Function}('{"array_bigint_1": [18]}')),(249, ${variantV2Function}('{"array_bigint_1": [19]}')),(250, ${variantV2Function}('{"array_bigint_1": [20]}')),(251, ${variantV2Function}('{"array_bigint_1": [21]}')); """

    check_table();

    sql """ insert into ${tableName}  values (252, ${variantV2Function}('{"array_largeint_1": [1, null]}')),(253, ${variantV2Function}('{"array_largeint_1": [2]}')),(254, ${variantV2Function}('{"array_largeint_1": [3]}')),(255, ${variantV2Function}('{"array_largeint_1": [4]}')),
    (256, ${variantV2Function}('{"array_largeint_1": [5]}')),(257, ${variantV2Function}('{"array_largeint_1": [6]}')),(258, ${variantV2Function}('{"array_largeint_1": [7]}')),(259, ${variantV2Function}('{"array_largeint_1": [8]}')),(260, ${variantV2Function}('{"array_largeint_1": [9]}')),(261, ${variantV2Function}('{"array_largeint_1": [10]}')),
    (262, ${variantV2Function}('{"array_largeint_1": [11]}')),(263, ${variantV2Function}('{"array_largeint_1": [12]}')),(264, ${variantV2Function}('{"array_largeint_1": [13]}')),(265, ${variantV2Function}('{"array_largeint_1": [14]}')),(266, ${variantV2Function}('{"array_largeint_1": [15]}')),(267, ${variantV2Function}('{"array_largeint_1": [16]}')),
    (268, ${variantV2Function}('{"array_largeint_1": [17]}')),(269, ${variantV2Function}('{"array_largeint_1": [18]}')),(270, ${variantV2Function}('{"array_largeint_1": [19]}')),(271, ${variantV2Function}('{"array_largeint_1": [20]}')),(272, ${variantV2Function}('{"array_largeint_1": [21]}')),(273, ${variantV2Function}('{"array_largeint_1": [22]}')); """

    check_table();

    sql """ insert into ${tableName}  values (274, ${variantV2Function}('{"array_char_1": ["1"]}')),(275, ${variantV2Function}('{"array_char_1": ["2"]}')),(276, ${variantV2Function}('{"array_char_1": ["3"]}')),(277, ${variantV2Function}('{"array_char_1": ["4"]}')),
    (278, ${variantV2Function}('{"array_char_1": ["5"]}')),(279, ${variantV2Function}('{"array_char_1": ["6"]}')),(280, ${variantV2Function}('{"array_char_1": ["7"]}')),(281, ${variantV2Function}('{"array_char_1": ["8"]}')),(282, ${variantV2Function}('{"array_char_1": ["9"]}')),(283, ${variantV2Function}('{"array_char_1": ["10"]}')),
    (284, ${variantV2Function}('{"array_char_1": ["11"]}')),(285, ${variantV2Function}('{"array_char_1": ["12"]}')),(286, ${variantV2Function}('{"array_char_1": ["13"]}')),(287, ${variantV2Function}('{"array_char_1": ["14"]}')),(288, ${variantV2Function}('{"array_char_1": ["15"]}')),(289, ${variantV2Function}('{"array_char_1": ["16"]}')),
    (290, ${variantV2Function}('{"array_char_1": ["17"]}')),(291, ${variantV2Function}('{"array_char_1": ["18"]}')),(292, ${variantV2Function}('{"array_char_1": ["19"]}')),(293, ${variantV2Function}('{"array_char_1": ["20"]}')),(294, ${variantV2Function}('{"array_char_1": ["21"]}')),(295, ${variantV2Function}('{"array_char_1": ["22"]}')),
    (296, ${variantV2Function}('{"array_char_1": ["23"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (297, ${variantV2Function}('{"array_string_1": ["1"]}')),(298, ${variantV2Function}('{"array_string_1": ["2"]}')),(299, ${variantV2Function}('{"array_string_1": ["3"]}')),(300, ${variantV2Function}('{"array_string_1": ["4"]}')),
    (301, ${variantV2Function}('{"array_string_1": ["5"]}')),(302, ${variantV2Function}('{"array_string_1": ["6"]}')),(303, ${variantV2Function}('{"array_string_1": ["7"]}')),(304, ${variantV2Function}('{"array_string_1": ["8"]}')),(305, ${variantV2Function}('{"array_string_1": ["9"]}')),(306, ${variantV2Function}('{"array_string_1": ["10"]}')),
    (307, ${variantV2Function}('{"array_string_1": ["11"]}')),(308, ${variantV2Function}('{"array_string_1": ["12"]}')),(309, ${variantV2Function}('{"array_string_1": ["13"]}')),(310, ${variantV2Function}('{"array_string_1": ["14"]}')),(311, ${variantV2Function}('{"array_string_1": ["15"]}')),(312, ${variantV2Function}('{"array_string_1": ["16"]}')),
    (313, ${variantV2Function}('{"array_string_1": ["17"]}')),(314, ${variantV2Function}('{"array_string_1": ["18"]}')),(315, ${variantV2Function}('{"array_string_1": ["19"]}')),(316, ${variantV2Function}('{"array_string_1": ["20"]}')),(317, ${variantV2Function}('{"array_string_1": ["21"]}')),(318, ${variantV2Function}('{"array_string_1": ["22"]}')),
    (319, ${variantV2Function}('{"array_string_1": ["23"]}')),(320, ${variantV2Function}('{"array_string_1": ["24"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (321, ${variantV2Function}('{"array_float_1": [1.12]}')),(322, ${variantV2Function}('{"array_float_1": [2.12]}')),(323, ${variantV2Function}('{"array_float_1": [3.12]}')),(324, ${variantV2Function}('{"array_float_1": [4.12]}')),
    (325, ${variantV2Function}('{"array_float_1": [5.12]}')),(326, ${variantV2Function}('{"array_float_1": [6.12]}')),(327, ${variantV2Function}('{"array_float_1": [7.12]}')),(328, ${variantV2Function}('{"array_float_1": [8.12]}')),(329, ${variantV2Function}('{"array_float_1": [9.12]}')),(330, ${variantV2Function}('{"array_float_1": [10.12]}')),
    (331, ${variantV2Function}('{"array_float_1": [11.12]}')),(332, ${variantV2Function}('{"array_float_1": [12.12]}')),(333, ${variantV2Function}('{"array_float_1": [13.12]}')),(334, ${variantV2Function}('{"array_float_1": [14.12]}')),(335, ${variantV2Function}('{"array_float_1": [15.12]}')),(336, ${variantV2Function}('{"array_float_1": [16.12]}')),
    (337, ${variantV2Function}('{"array_float_1": [17.12]}')),(338, ${variantV2Function}('{"array_float_1": [18.12]}')),(339, ${variantV2Function}('{"array_float_1": [19.12]}')),(340, ${variantV2Function}('{"array_float_1": [20.12]}')),(341, ${variantV2Function}('{"array_float_1": [21.12]}')),(342, ${variantV2Function}('{"array_float_1": [22.12]}')),
    (343, ${variantV2Function}('{"array_float_1": [23.12]}')),(344, ${variantV2Function}('{"array_float_1": [24.12]}')),(345, ${variantV2Function}('{"array_float_1": [25.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (346, ${variantV2Function}('{"array_double_1": [1.12]}')),(347, ${variantV2Function}('{"array_double_1": [2.12]}')),(348, ${variantV2Function}('{"array_double_1": [3.12]}')),(349, ${variantV2Function}('{"array_double_1": [4.12]}')),
    (350, ${variantV2Function}('{"array_double_1": [5.12]}')),(351, ${variantV2Function}('{"array_double_1": [6.12]}')),(352, ${variantV2Function}('{"array_double_1": [7.12]}')),(353, ${variantV2Function}('{"array_double_1": [8.12]}')),(354, ${variantV2Function}('{"array_double_1": [9.12]}')),(355, ${variantV2Function}('{"array_double_1": [10.12]}')),
    (356, ${variantV2Function}('{"array_double_1": [11.12]}')),(357, ${variantV2Function}('{"array_double_1": [12.12]}')),(358, ${variantV2Function}('{"array_double_1": [13.12]}')),(359, ${variantV2Function}('{"array_double_1": [14.12]}')),(360, ${variantV2Function}('{"array_double_1": [15.12]}')),(361, ${variantV2Function}('{"array_double_1": [16.12]}')),
    (362, ${variantV2Function}('{"array_double_1": [17.12]}')),(363, ${variantV2Function}('{"array_double_1": [18.12]}')),(364, ${variantV2Function}('{"array_double_1": [19.12]}')),(365, ${variantV2Function}('{"array_double_1": [20.12]}')),(366, ${variantV2Function}('{"array_double_1": [21.12]}')),(367, ${variantV2Function}('{"array_double_1": [22.12]}')),
    (368, ${variantV2Function}('{"array_double_1": [23.12]}')),(369, ${variantV2Function}('{"array_double_1": [24.12]}')),(370, ${variantV2Function}('{"array_double_1": [25.12]}')),(371, ${variantV2Function}('{"array_double_1": [26.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (372, ${variantV2Function}('{"array_decimal32_1": [1.12]}')),(373, ${variantV2Function}('{"array_decimal32_1": [2.12]}')),(374, ${variantV2Function}('{"array_decimal32_1": [3.12]}')),(375, ${variantV2Function}('{"array_decimal32_1": [4.12]}')),
    (376, ${variantV2Function}('{"array_decimal32_1": [5.12]}')),(377, ${variantV2Function}('{"array_decimal32_1": [6.12]}')),(378, ${variantV2Function}('{"array_decimal32_1": [7.12]}')),(379, ${variantV2Function}('{"array_decimal32_1": [8.12]}')),(380, ${variantV2Function}('{"array_decimal32_1": [9.12]}')),(381, ${variantV2Function}('{"array_decimal32_1": [10.12]}')),
    (382, ${variantV2Function}('{"array_decimal32_1": [11.12]}')),(383, ${variantV2Function}('{"array_decimal32_1": [12.12]}')),(384, ${variantV2Function}('{"array_decimal32_1": [13.12]}')),(385, ${variantV2Function}('{"array_decimal32_1": [14.12]}')),(386, ${variantV2Function}('{"array_decimal32_1": [15.12]}')),(387, ${variantV2Function}('{"array_decimal32_1": [16.12]}')),
    (388, ${variantV2Function}('{"array_decimal32_1": [17.12]}')),(389, ${variantV2Function}('{"array_decimal32_1": [18.12]}')),(390, ${variantV2Function}('{"array_decimal32_1": [19.12]}')),(391, ${variantV2Function}('{"array_decimal32_1": [20.12]}')),(392, ${variantV2Function}('{"array_decimal32_1": [21.12]}')),(393, ${variantV2Function}('{"array_decimal32_1": [22.12]}')),
    (394, ${variantV2Function}('{"array_decimal32_1": [23.12]}')),(395, ${variantV2Function}('{"array_decimal32_1": [24.12]}')),(396, ${variantV2Function}('{"array_decimal32_1": [25.12]}')),(397, ${variantV2Function}('{"array_decimal32_1": [26.12]}')),(398, ${variantV2Function}('{"array_decimal32_1": [27.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (399, ${variantV2Function}('{"array_decimal64_1": [1.12]}')),(400, ${variantV2Function}('{"array_decimal64_1": [2.12]}')),(401, ${variantV2Function}('{"array_decimal64_1": [3.12]}')),(402, ${variantV2Function}('{"array_decimal64_1": [4.12]}')),
    (403, ${variantV2Function}('{"array_decimal64_1": [5.12]}')),(404, ${variantV2Function}('{"array_decimal64_1": [6.12]}')),(405, ${variantV2Function}('{"array_decimal64_1": [7.12]}')),(406, ${variantV2Function}('{"array_decimal64_1": [8.12]}')),(407, ${variantV2Function}('{"array_decimal64_1": [9.12]}')),(408, ${variantV2Function}('{"array_decimal64_1": [10.12]}')),
    (409, ${variantV2Function}('{"array_decimal64_1": [11.12]}')),(410, ${variantV2Function}('{"array_decimal64_1": [12.12]}')),(411, ${variantV2Function}('{"array_decimal64_1": [13.12]}')),(412, ${variantV2Function}('{"array_decimal64_1": [14.12]}')),(413, ${variantV2Function}('{"array_decimal64_1": [15.12]}')),(414, ${variantV2Function}('{"array_decimal64_1": [16.12]}')),
    (415, ${variantV2Function}('{"array_decimal64_1": [17.12]}')),(416, ${variantV2Function}('{"array_decimal64_1": [18.12]}')),(417, ${variantV2Function}('{"array_decimal64_1": [19.12]}')),(418, ${variantV2Function}('{"array_decimal64_1": [20.12]}')),(419, ${variantV2Function}('{"array_decimal64_1": [21.12]}')),(420, ${variantV2Function}('{"array_decimal64_1": [22.12]}')),
    (421, ${variantV2Function}('{"array_decimal64_1": [23.12]}')),(422, ${variantV2Function}('{"array_decimal64_1": [24.12]}')),(423, ${variantV2Function}('{"array_decimal64_1": [25.12]}')),(424, ${variantV2Function}('{"array_decimal64_1": [26.12]}')),(425, ${variantV2Function}('{"array_decimal64_1": [27.12]}')),(426, ${variantV2Function}('{"array_decimal64_1": [28.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (427, ${variantV2Function}('{"array_decimal128_1": [1.12]}')),(428, ${variantV2Function}('{"array_decimal128_1": [2.12]}')),(429, ${variantV2Function}('{"array_decimal128_1": [3.12]}')),(430, ${variantV2Function}('{"array_decimal128_1": [4.12]}')),
    (431, ${variantV2Function}('{"array_decimal128_1": [5.12]}')),(432, ${variantV2Function}('{"array_decimal128_1": [6.12]}')),(433, ${variantV2Function}('{"array_decimal128_1": [7.12]}')),(434, ${variantV2Function}('{"array_decimal128_1": [8.12]}')),(435, ${variantV2Function}('{"array_decimal128_1": [9.12]}')),(436, ${variantV2Function}('{"array_decimal128_1": [10.12]}')),
    (437, ${variantV2Function}('{"array_decimal128_1": [11.12]}')),(438, ${variantV2Function}('{"array_decimal128_1": [12.12]}')),(439, ${variantV2Function}('{"array_decimal128_1": [13.12]}')),(440, ${variantV2Function}('{"array_decimal128_1": [14.12]}')),(441, ${variantV2Function}('{"array_decimal128_1": [15.12]}')),(442, ${variantV2Function}('{"array_decimal128_1": [16.12]}')),
    (443, ${variantV2Function}('{"array_decimal128_1": [17.12]}')),(444, ${variantV2Function}('{"array_decimal128_1": [18.12]}')),(445, ${variantV2Function}('{"array_decimal128_1": [19.12]}')),(446, ${variantV2Function}('{"array_decimal128_1": [20.12]}')),(447, ${variantV2Function}('{"array_decimal128_1": [21.12]}')),(448, ${variantV2Function}('{"array_decimal128_1": [22.12]}')),
    (449, ${variantV2Function}('{"array_decimal128_1": [23.12]}')),(450, ${variantV2Function}('{"array_decimal128_1": [24.12]}')),(451, ${variantV2Function}('{"array_decimal128_1": [25.12]}')),(452, ${variantV2Function}('{"array_decimal128_1": [26.12]}')),(453, ${variantV2Function}('{"array_decimal128_1": [27.12]}')),(454, ${variantV2Function}('{"array_decimal128_1": [28.12]}')),
    (455, ${variantV2Function}('{"array_decimal128_1": [29.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (456, ${variantV2Function}('{"array_decimal256_1": [1.12]}')),(457, ${variantV2Function}('{"array_decimal256_1": [2.12]}')),(458, ${variantV2Function}('{"array_decimal256_1": [3.12]}')),(459, ${variantV2Function}('{"array_decimal256_1": [4.12]}')),
    (460, ${variantV2Function}('{"array_decimal256_1": [5.12]}')),(461, ${variantV2Function}('{"array_decimal256_1": [6.12]}')),(462, ${variantV2Function}('{"array_decimal256_1": [7.12]}')),(463, ${variantV2Function}('{"array_decimal256_1": [8.12]}')),(464, ${variantV2Function}('{"array_decimal256_1": [9.12]}')),(465, ${variantV2Function}('{"array_decimal256_1": [10.12]}')),
    (466, ${variantV2Function}('{"array_decimal256_1": [11.12]}')),(467, ${variantV2Function}('{"array_decimal256_1": [12.12]}')),(468, ${variantV2Function}('{"array_decimal256_1": [13.12]}')),(469, ${variantV2Function}('{"array_decimal256_1": [14.12]}')),(470, ${variantV2Function}('{"array_decimal256_1": [15.12]}')),(471, ${variantV2Function}('{"array_decimal256_1": [16.12]}')),
    (472, ${variantV2Function}('{"array_decimal256_1": [17.12]}')),(473, ${variantV2Function}('{"array_decimal256_1": [18.12]}')),(474, ${variantV2Function}('{"array_decimal256_1": [19.12]}')),(475, ${variantV2Function}('{"array_decimal256_1": [20.12]}')),(476, ${variantV2Function}('{"array_decimal256_1": [21.12]}')),(477, ${variantV2Function}('{"array_decimal256_1": [22.12]}')),
    (478, ${variantV2Function}('{"array_decimal256_1": [23.12]}')),(479, ${variantV2Function}('{"array_decimal256_1": [24.12]}')),(480, ${variantV2Function}('{"array_decimal256_1": [25.12]}')),(481, ${variantV2Function}('{"array_decimal256_1": [26.12]}')),(482, ${variantV2Function}('{"array_decimal256_1": [27.12]}')),(483, ${variantV2Function}('{"array_decimal256_1": [28.12]}')),
    (484, ${variantV2Function}('{"array_decimal256_1": [29.12]}')),(485, ${variantV2Function}('{"array_decimal256_1": [30.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (486, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:00"]}')),(487, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:01"]}')),(488, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:02"]}')),
    (489, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:03"]}')),(490, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:04"]}')),(491, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:05"]}')),(492, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:06"]}')),
    (493, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(494, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(495, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(496, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (497, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(498, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(499, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(500, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (501, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(502, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(503, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(504, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (505, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(506, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(507, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(508, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (509, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(510, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(511, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(512, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (513, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(514, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(515, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(516, ${variantV2Function}('{"array_datetime_1": ["2021-01-01 00:00:10"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (517, ${variantV2Function}('{"array_date_1": ["2021-01-01"]}')),(518, ${variantV2Function}('{"array_date_1": ["2021-01-02"]}')),(519, ${variantV2Function}('{"array_date_1": ["2021-01-03"]}')),(520, ${variantV2Function}('{"array_date_1": ["2021-01-04"]}')),
    (521, ${variantV2Function}('{"array_date_1": ["2021-01-05"]}')),(522, ${variantV2Function}('{"array_date_1": ["2021-01-06"]}')),(523, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(524, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(525, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(526, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (527, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(528, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(529, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(530, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (531, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(532, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(533, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(534, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (535, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(536, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(537, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(538, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (539, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(540, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(541, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(542, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (543, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(544, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')),(545, ${variantV2Function}('{"array_date_1": ["2021-01-09"]}')),(546, ${variantV2Function}('{"array_date_1": ["2021-01-10"]}')),
    (547, ${variantV2Function}('{"array_date_1": ["2021-01-07"]}')),(548, ${variantV2Function}('{"array_date_1": ["2021-01-08"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (549, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.1"]}')),(550, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.2"]}')),(551, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.3"]}')),(552, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.4"]}')),
    (553, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.5"]}')),(554, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.6"]}')),(555, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(556, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(557, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(558, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (559, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(560, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(561, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(562, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (563, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(564, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(565, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(566, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (567, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(568, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(569, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(570, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (571, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(572, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(573, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(574, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (575, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(576, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(577, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')),(578, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.10"]}')),
    (579, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.7"]}')),(580, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.8"]}')),(581, ${variantV2Function}('{"array_ipv4_1": ["192.168.1.9"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (582, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"]}')),(583, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7335"]}')),
    (584, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7336"]}')),(585, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7337"]}')),(586, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7338"]}')),
    (587, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7339"]}')),(588, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733a"]}')),(589, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733b"]}')),
    (590, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733c"]}')),(591, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733d"]}')),(592, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733e"]}')),
    (593, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(594, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(595, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (596, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(597, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(598, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (599, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(600, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(601, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (602, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(603, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(604, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (605, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(606, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(607, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (608, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(609, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(610, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (611, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(612, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(613, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (614, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(615, ${variantV2Function}('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (616, ${variantV2Function}('{"other_1": "1"}')),(617, ${variantV2Function}('{"other_1": "2"}')),(618, ${variantV2Function}('{"other_1": "3"}')),(619, ${variantV2Function}('{"other_1": "4"}')),(620, ${variantV2Function}('{"other_1": "5"}')),(621, ${variantV2Function}('{"other_1": "6"}')),(622, ${variantV2Function}('{"other_1": "7"}')),(623, ${variantV2Function}('{"other_1": "8"}')),(624, ${variantV2Function}('{"other_1": "9"}')),(625, ${variantV2Function}('{"other_1": "10"}')),
    (626, ${variantV2Function}('{"other_1": "11"}')),(627, ${variantV2Function}('{"other_1": "12"}')),(628, ${variantV2Function}('{"other_1": "13"}')),(629, ${variantV2Function}('{"other_1": "14"}')),(630, ${variantV2Function}('{"other_1": "15"}')),(631, ${variantV2Function}('{"other_1": "16"}')),(632, ${variantV2Function}('{"other_1": "17"}')),(633, ${variantV2Function}('{"other_1": "18"}')),(634, ${variantV2Function}('{"other_1": "19"}')),(635, ${variantV2Function}('{"other_1": "20"}')),
    (636, ${variantV2Function}('{"other_1": "21"}')),(637, ${variantV2Function}('{"other_1": "22"}')),(638, ${variantV2Function}('{"other_1": "23"}')),(639, ${variantV2Function}('{"other_1": "24"}')),(640, ${variantV2Function}('{"other_1": "25"}')),(641, ${variantV2Function}('{"other_1": "26"}')),(642, ${variantV2Function}('{"other_1": "27"}')),(643, ${variantV2Function}('{"other_1": "28"}')),(644, ${variantV2Function}('{"other_1": "29"}')),(645, ${variantV2Function}('{"other_1": "30"}')),
    (646, ${variantV2Function}('{"other_1": "31"}')),(647, ${variantV2Function}('{"other_1": "32"}')),(648, ${variantV2Function}('{"other_1": "33"}')),(649, ${variantV2Function}('{"other_1": "34"}')),(650, ${variantV2Function}('{"other_1": "35"}')); """

    check_table();
    }
}
