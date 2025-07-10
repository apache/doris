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

suite("test_all_prdefine_type_to_sparse", "p0"){ 

    sql """ set describe_extend_variant_column = true """

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
                    "variant_max_subcolumns_count" = "1"
                )
            > NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """

    sql """
         INSERT INTO ${tableName} VALUES
        (0,
            '{
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
            }'
        ),
        (1,
            '{"other_1": "1"}'
        ); 
    """

    qt_sql """ select variant_type(var) from ${tableName} limit 1"""
    qt_sql """ select var from ${tableName} order by id """


    def check_table = {
        def before_result = sql """ select var from ${tableName} order by id """
        log.info("before_result: ${before_result}")
        qt_sql_compaction_before """ desc ${tableName} """

        trigger_and_wait_compaction(tableName, "full")

        def after_result = sql """ select var from ${tableName} order by id """
        log.info("after_result: ${after_result}")
        assertTrue(before_result.toString() == after_result.toString())
        
        qt_sql_compaction_after """ desc ${tableName} """
        qt_sql """ select var from ${tableName} order by id """
    }

    sql """ insert into ${tableName} values (2, '{"tinyint_1": 1}'),(3, '{"tinyint_1": 2}'); """

    check_table();

    sql """ insert into ${tableName} values (4, '{"smallint_1": 1}'),(5, '{"smallint_1": 2}'),(6, '{"smallint_1": 3}'); """

    check_table();

    sql """ insert into ${tableName}  values (7, '{"int_1": 1}'),(8, '{"int_1": 2}'),(9, '{"int_1": 3}'),(10, '{"int_1": 4}'); """

    check_table();

    sql """ insert into ${tableName}  values (11, '{"bigint_1": 1}'),(12, '{"bigint_1": 2}'),(13, '{"bigint_1": 3}'),(14, '{"bigint_1": 4}'),(15, '{"bigint_1": 5}'); """

    check_table();

    sql """ insert into ${tableName}  values (16, '{"largeint_1": 1}'),(17, '{"largeint_1": 2}'),(18, '{"largeint_1": 3}'),(19, '{"largeint_1": 4}'),(20, '{"largeint_1": 5}'),(21, '{"largeint_1": 6}'); """

    check_table();

    sql """ insert into ${tableName}  values (22, '{"char_1": "1"}'),(23, '{"char_1": "2"}'),(24, '{"char_1": "3"}'),(25, '{"char_1": "4"}'),(26, '{"char_1": "5"}'),(27, '{"char_1": "6"}'),(28, '{"char_1": "7"}'); """

    check_table();

    sql """ insert into ${tableName}  values (29, '{"string_1": "1"}'),(30, '{"string_1": "2"}'),(31, '{"string_1": "3"}'),(32, '{"string_1": "4"}'),(33, '{"string_1": "5"}'),
    (34, '{"string_1": "6"}'),(35, '{"string_1": "7"}'),(36, '{"string_1": "8"}'); """

    check_table();

    sql """ insert into ${tableName}  values (37, '{"float_1": 1.12}'),(38, '{"float_1": 2.12}'),(39, '{"float_1": 3.12}'),(40, '{"float_1": 4.12}'),(41, '{"float_1": 5.12}'),
    (42, '{"float_1": 6.12}'),(43, '{"float_1": 7.12}'),(44, '{"float_1": 8.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (45, '{"double_1": 1.12}'),(46, '{"double_1": 2.12}'),(47, '{"double_1": 3.12}'),(48, '{"double_1": 4.12}'),(49, '{"double_1": 5.12}'),
    (50, '{"double_1": 6.12}'),(51, '{"double_1": 7.12}'),(52, '{"double_1": 8.12}'),(53, '{"double_1": 9.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (54, '{"decimal32_1": 1.12}'),(55, '{"decimal32_1": 2.12}'),(56, '{"decimal32_1": 3.12}'),(57, '{"decimal32_1": 4.12}'),(58, '{"decimal32_1": 5.12}'),
    (59, '{"decimal32_1": 6.12}'),(60, '{"decimal32_1": 7.12}'),(61, '{"decimal32_1": 8.12}'),(62, '{"decimal32_1": 9.12}'),(63, '{"decimal32_1": 10.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (64, '{"decimal64_1": 1.12}'),(65, '{"decimal64_1": 2.12}'),(66, '{"decimal64_1": 3.12}'),(67, '{"decimal64_1": 4.12}'),(68, '{"decimal64_1": 5.12}'),
    (69, '{"decimal64_1": 6.12}'),(70, '{"decimal64_1": 7.12}'),(71, '{"decimal64_1": 8.12}'),(72, '{"decimal64_1": 9.12}'),(73, '{"decimal64_1": 10.12}'),(74, '{"decimal64_1": 11.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (75, '{"decimal128_1": 1.12}'),(76, '{"decimal128_1": 2.12}'),(77, '{"decimal128_1": 3.12}'),(78, '{"decimal128_1": 4.12}'),(79, '{"decimal128_1": 5.12}'),
    (80, '{"decimal128_1": 6.12}'),(81, '{"decimal128_1": 7.12}'),(82, '{"decimal128_1": 8.12}'),(83, '{"decimal128_1": 9.12}'),(84, '{"decimal128_1": 10.12}'),(85, '{"decimal128_1": 11.12}'),
    (86, '{"decimal128_1": 12.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (87, '{"decimal256_1": 1.12}'),(88, '{"decimal256_1": 2.12}'),(89, '{"decimal256_1": 3.12}'),(90, '{"decimal256_1": 4.12}'),(91, '{"decimal256_1": 5.12}'),
    (92, '{"decimal256_1": 6.12}'),(93, '{"decimal256_1": 7.12}'),(94, '{"decimal256_1": 8.12}'),(95, '{"decimal256_1": 9.12}'),(96, '{"decimal256_1": 10.12}'),(97, '{"decimal256_1": 11.12}'),
    (98, '{"decimal256_1": 12.12}'),(99, '{"decimal256_1": 13.12}'); """

    check_table();

    sql """ insert into ${tableName}  values (100, '{"datetime_1": "2021-01-01 00:00:00"}'),(101, '{"datetime_1": "2021-01-01 00:00:01"}'),(102, '{"datetime_1": "2021-01-01 00:00:02"}'),
    (103, '{"datetime_1": "2021-01-01 00:00:03"}'),(104, '{"datetime_1": "2021-01-01 00:00:04"}'),(105, '{"datetime_1": "2021-01-01 00:00:05"}'),(106, '{"datetime_1": "2021-01-01 00:00:06"}'),
    (107, '{"datetime_1": "2021-01-01 00:00:07"}'),(108, '{"datetime_1": "2021-01-01 00:00:08"}'),(109, '{"datetime_1": "2021-01-01 00:00:09"}'),(110, '{"datetime_1": "2021-01-01 00:00:10"}'),
    (111, '{"datetime_1": "2021-01-01 00:00:07"}'),(112, '{"datetime_1": "2021-01-01 00:00:08"}'); """

    check_table();

    sql """ insert into ${tableName}  values (113, '{"date_1": "2021-01-01"}'),(114, '{"date_1": "2021-01-02"}'),(115, '{"date_1": "2021-01-03"}'),(116, '{"date_1": "2021-01-04"}'),
    (117, '{"date_1": "2021-01-05"}'),(118, '{"date_1": "2021-01-06"}'),(119, '{"date_1": "2021-01-07"}'),(120, '{"date_1": "2021-01-08"}'),(121, '{"date_1": "2021-01-09"}'),(122, '{"date_1": "2021-01-10"}'),
    (123, '{"date_1": "2021-01-07"}'),(124, '{"date_1": "2021-01-08"}'),(125, '{"date_1": "2021-01-09"}'),(126, '{"date_1": "2021-01-10"}'); """

    check_table();

    sql """ insert into ${tableName}  values (127, '{"ipv4_1": "192.168.1.1"}'),(128, '{"ipv4_1": "192.168.1.2"}'),(129, '{"ipv4_1": "192.168.1.3"}'),(130, '{"ipv4_1": "192.168.1.4"}'),
    (131, '{"ipv4_1": "192.168.1.5"}'),(132, '{"ipv4_1": "192.168.1.6"}'),(133, '{"ipv4_1": "192.168.1.7"}'),(134, '{"ipv4_1": "192.168.1.8"}'),(135, '{"ipv4_1": "192.168.1.9"}'),(136, '{"ipv4_1": "192.168.1.10"}'),
    (137, '{"ipv4_1": "192.168.1.7"}'),(138, '{"ipv4_1": "192.168.1.8"}'),(139, '{"ipv4_1": "192.168.1.9"}'),(140, '{"ipv4_1": "192.168.1.10"}'),(141, '{"ipv4_1": "192.168.1.11"}'); """

    check_table();

    sql """ insert into ${tableName}  values (142, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}'),(143, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7335"}'),
    (144, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7336"}'),(145, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7337"}'),(146, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7338"}'),
    (147, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7339"}'),(148, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733a"}'),(149, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733b"}'),
    (150, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733c"}'),(151, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733d"}'),(152, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733e"}'),
    (153, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}'),(154, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}'),(155, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7341"}'),
    (156, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}'),(157, '{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}'); """

    check_table();

    sql """ insert into ${tableName}  values (158, '{"array_boolean_1": [true]}'),(159, '{"array_boolean_1": [false]}'),(160, '{"array_boolean_1": [true]}'),(161, '{"array_boolean_1": [false]}'),
    (162, '{"array_boolean_1": [true]}'),(163, '{"array_boolean_1": [false]}'),(164, '{"array_boolean_1": [true]}'),(165, '{"array_boolean_1": [false]}'),(166, '{"array_boolean_1": [true]}'),(167, '{"array_boolean_1": [false]}'),
    (168, '{"array_boolean_1": [true]}'),(169, '{"array_boolean_1": [false]}'),(170, '{"array_boolean_1": [true]}'),(171, '{"array_boolean_1": [false]}'),(172, '{"array_boolean_1": [true]}'),(173, '{"array_boolean_1": [false]}');"""

    check_table();

    sql """ insert into ${tableName}  values (174, '{"array_tinyint_1": [1]}'),(175, '{"array_tinyint_1": [2]}'),(176, '{"array_tinyint_1": [3]}'),(177, '{"array_tinyint_1": [4]}'),
    (178, '{"array_tinyint_1": [5]}'),(179, '{"array_tinyint_1": [6]}'),(180, '{"array_tinyint_1": [7]}'),(181, '{"array_tinyint_1": [8]}'),(182, '{"array_tinyint_1": [9]}'),(183, '{"array_tinyint_1": [10]}'),
    (184, '{"array_tinyint_1": [11]}'),(185, '{"array_tinyint_1": [12]}'),(186, '{"array_tinyint_1": [13]}'),(187, '{"array_tinyint_1": [14]}'),(188, '{"array_tinyint_1": [15]}'),(189, '{"array_tinyint_1": [16]}'),
    (190, '{"array_tinyint_1": [17]}'),(191, '{"array_tinyint_1": [18]}'); """

    check_table();
    
    sql """ insert into ${tableName}  values (192, '{"array_smallint_1": [1]}'),(193, '{"array_smallint_1": [2, null]}'),(194, '{"array_smallint_1": [3]}'),(195, '{"array_smallint_1": [4]}'),
    (196, '{"array_smallint_1": [5]}'),(197, '{"array_smallint_1": [6]}'),(198, '{"array_smallint_1": [7]}'),(199, '{"array_smallint_1": [8]}'),(200, '{"array_smallint_1": [9]}'),(201, '{"array_smallint_1": [10]}'),
    (202, '{"array_smallint_1": [11]}'),(203, '{"array_smallint_1": [12]}'),(204, '{"array_smallint_1": [13]}'),(205, '{"array_smallint_1": [14]}'),(206, '{"array_smallint_1": [15]}'),(207, '{"array_smallint_1": [16]}'),
    (208, '{"array_smallint_1": [17]}'),(209, '{"array_smallint_1": [18]}'),(210, '{"array_smallint_1": [19]}'); """

    check_table();

    sql """ insert into ${tableName}  values (211, '{"array_int_1": [1]}'),(212, '{"array_int_1": [2]}'),(213, '{"array_int_1": [3]}'),(214, '{"array_int_1": [4]}'),
    (215, '{"array_int_1": [5]}'),(216, '{"array_int_1": [6]}'),(217, '{"array_int_1": [7]}'),(218, '{"array_int_1": [8]}'),(219, '{"array_int_1": [9]}'),(220, '{"array_int_1": [10]}'),
    (221, '{"array_int_1": [11]}'),(222, '{"array_int_1": [12]}'),(223, '{"array_int_1": [13]}'),(224, '{"array_int_1": [14]}'),(225, '{"array_int_1": [15]}'),(226, '{"array_int_1": [16]}'),
    (227, '{"array_int_1": [17]}'),(228, '{"array_int_1": [18]}'),(229, '{"array_int_1": [19]}'),(230, '{"array_int_1": [20]}'); """

    check_table();

    sql """ insert into ${tableName}  values (231, '{"array_bigint_1": [1]}'),(232, '{"array_bigint_1": [2]}'),(233, '{"array_bigint_1": [3]}'),(234, '{"array_bigint_1": [4]}'),
    (235, '{"array_bigint_1": [5]}'),(236, '{"array_bigint_1": [6]}'),(237, '{"array_bigint_1": [7]}'),(238, '{"array_bigint_1": [8]}'),(239, '{"array_bigint_1": [9]}'),(240, '{"array_bigint_1": [10]}'),
    (241, '{"array_bigint_1": [11]}'),(242, '{"array_bigint_1": [12]}'),(243, '{"array_bigint_1": [13]}'),(244, '{"array_bigint_1": [14]}'),(245, '{"array_bigint_1": [15]}'),(246, '{"array_bigint_1": [16]}'),
    (247, '{"array_bigint_1": [17]}'),(248, '{"array_bigint_1": [18]}'),(249, '{"array_bigint_1": [19]}'),(250, '{"array_bigint_1": [20]}'),(251, '{"array_bigint_1": [21]}'); """

    check_table();

    sql """ insert into ${tableName}  values (252, '{"array_largeint_1": [1, null]}'),(253, '{"array_largeint_1": [2]}'),(254, '{"array_largeint_1": [3]}'),(255, '{"array_largeint_1": [4]}'),
    (256, '{"array_largeint_1": [5]}'),(257, '{"array_largeint_1": [6]}'),(258, '{"array_largeint_1": [7]}'),(259, '{"array_largeint_1": [8]}'),(260, '{"array_largeint_1": [9]}'),(261, '{"array_largeint_1": [10]}'),
    (262, '{"array_largeint_1": [11]}'),(263, '{"array_largeint_1": [12]}'),(264, '{"array_largeint_1": [13]}'),(265, '{"array_largeint_1": [14]}'),(266, '{"array_largeint_1": [15]}'),(267, '{"array_largeint_1": [16]}'),
    (268, '{"array_largeint_1": [17]}'),(269, '{"array_largeint_1": [18]}'),(270, '{"array_largeint_1": [19]}'),(271, '{"array_largeint_1": [20]}'),(272, '{"array_largeint_1": [21]}'),(273, '{"array_largeint_1": [22]}'); """

    check_table();

    sql """ insert into ${tableName}  values (274, '{"array_char_1": ["1"]}'),(275, '{"array_char_1": ["2"]}'),(276, '{"array_char_1": ["3"]}'),(277, '{"array_char_1": ["4"]}'),
    (278, '{"array_char_1": ["5"]}'),(279, '{"array_char_1": ["6"]}'),(280, '{"array_char_1": ["7"]}'),(281, '{"array_char_1": ["8"]}'),(282, '{"array_char_1": ["9"]}'),(283, '{"array_char_1": ["10"]}'),
    (284, '{"array_char_1": ["11"]}'),(285, '{"array_char_1": ["12"]}'),(286, '{"array_char_1": ["13"]}'),(287, '{"array_char_1": ["14"]}'),(288, '{"array_char_1": ["15"]}'),(289, '{"array_char_1": ["16"]}'),
    (290, '{"array_char_1": ["17"]}'),(291, '{"array_char_1": ["18"]}'),(292, '{"array_char_1": ["19"]}'),(293, '{"array_char_1": ["20"]}'),(294, '{"array_char_1": ["21"]}'),(295, '{"array_char_1": ["22"]}'),
    (296, '{"array_char_1": ["23"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (297, '{"array_string_1": ["1"]}'),(298, '{"array_string_1": ["2"]}'),(299, '{"array_string_1": ["3"]}'),(300, '{"array_string_1": ["4"]}'),
    (301, '{"array_string_1": ["5"]}'),(302, '{"array_string_1": ["6"]}'),(303, '{"array_string_1": ["7"]}'),(304, '{"array_string_1": ["8"]}'),(305, '{"array_string_1": ["9"]}'),(306, '{"array_string_1": ["10"]}'),
    (307, '{"array_string_1": ["11"]}'),(308, '{"array_string_1": ["12"]}'),(309, '{"array_string_1": ["13"]}'),(310, '{"array_string_1": ["14"]}'),(311, '{"array_string_1": ["15"]}'),(312, '{"array_string_1": ["16"]}'),
    (313, '{"array_string_1": ["17"]}'),(314, '{"array_string_1": ["18"]}'),(315, '{"array_string_1": ["19"]}'),(316, '{"array_string_1": ["20"]}'),(317, '{"array_string_1": ["21"]}'),(318, '{"array_string_1": ["22"]}'),
    (319, '{"array_string_1": ["23"]}'),(320, '{"array_string_1": ["24"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (321, '{"array_float_1": [1.12]}'),(322, '{"array_float_1": [2.12]}'),(323, '{"array_float_1": [3.12]}'),(324, '{"array_float_1": [4.12]}'),
    (325, '{"array_float_1": [5.12]}'),(326, '{"array_float_1": [6.12]}'),(327, '{"array_float_1": [7.12]}'),(328, '{"array_float_1": [8.12]}'),(329, '{"array_float_1": [9.12]}'),(330, '{"array_float_1": [10.12]}'),
    (331, '{"array_float_1": [11.12]}'),(332, '{"array_float_1": [12.12]}'),(333, '{"array_float_1": [13.12]}'),(334, '{"array_float_1": [14.12]}'),(335, '{"array_float_1": [15.12]}'),(336, '{"array_float_1": [16.12]}'),
    (337, '{"array_float_1": [17.12]}'),(338, '{"array_float_1": [18.12]}'),(339, '{"array_float_1": [19.12]}'),(340, '{"array_float_1": [20.12]}'),(341, '{"array_float_1": [21.12]}'),(342, '{"array_float_1": [22.12]}'),
    (343, '{"array_float_1": [23.12]}'),(344, '{"array_float_1": [24.12]}'),(345, '{"array_float_1": [25.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (346, '{"array_double_1": [1.12]}'),(347, '{"array_double_1": [2.12]}'),(348, '{"array_double_1": [3.12]}'),(349, '{"array_double_1": [4.12]}'),
    (350, '{"array_double_1": [5.12]}'),(351, '{"array_double_1": [6.12]}'),(352, '{"array_double_1": [7.12]}'),(353, '{"array_double_1": [8.12]}'),(354, '{"array_double_1": [9.12]}'),(355, '{"array_double_1": [10.12]}'),
    (356, '{"array_double_1": [11.12]}'),(357, '{"array_double_1": [12.12]}'),(358, '{"array_double_1": [13.12]}'),(359, '{"array_double_1": [14.12]}'),(360, '{"array_double_1": [15.12]}'),(361, '{"array_double_1": [16.12]}'),
    (362, '{"array_double_1": [17.12]}'),(363, '{"array_double_1": [18.12]}'),(364, '{"array_double_1": [19.12]}'),(365, '{"array_double_1": [20.12]}'),(366, '{"array_double_1": [21.12]}'),(367, '{"array_double_1": [22.12]}'),
    (368, '{"array_double_1": [23.12]}'),(369, '{"array_double_1": [24.12]}'),(370, '{"array_double_1": [25.12]}'),(371, '{"array_double_1": [26.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (372, '{"array_decimal32_1": [1.12]}'),(373, '{"array_decimal32_1": [2.12]}'),(374, '{"array_decimal32_1": [3.12]}'),(375, '{"array_decimal32_1": [4.12]}'),
    (376, '{"array_decimal32_1": [5.12]}'),(377, '{"array_decimal32_1": [6.12]}'),(378, '{"array_decimal32_1": [7.12]}'),(379, '{"array_decimal32_1": [8.12]}'),(380, '{"array_decimal32_1": [9.12]}'),(381, '{"array_decimal32_1": [10.12]}'),
    (382, '{"array_decimal32_1": [11.12]}'),(383, '{"array_decimal32_1": [12.12]}'),(384, '{"array_decimal32_1": [13.12]}'),(385, '{"array_decimal32_1": [14.12]}'),(386, '{"array_decimal32_1": [15.12]}'),(387, '{"array_decimal32_1": [16.12]}'),
    (388, '{"array_decimal32_1": [17.12]}'),(389, '{"array_decimal32_1": [18.12]}'),(390, '{"array_decimal32_1": [19.12]}'),(391, '{"array_decimal32_1": [20.12]}'),(392, '{"array_decimal32_1": [21.12]}'),(393, '{"array_decimal32_1": [22.12]}'),
    (394, '{"array_decimal32_1": [23.12]}'),(395, '{"array_decimal32_1": [24.12]}'),(396, '{"array_decimal32_1": [25.12]}'),(397, '{"array_decimal32_1": [26.12]}'),(398, '{"array_decimal32_1": [27.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (399, '{"array_decimal64_1": [1.12]}'),(400, '{"array_decimal64_1": [2.12]}'),(401, '{"array_decimal64_1": [3.12]}'),(402, '{"array_decimal64_1": [4.12]}'),
    (403, '{"array_decimal64_1": [5.12]}'),(404, '{"array_decimal64_1": [6.12]}'),(405, '{"array_decimal64_1": [7.12]}'),(406, '{"array_decimal64_1": [8.12]}'),(407, '{"array_decimal64_1": [9.12]}'),(408, '{"array_decimal64_1": [10.12]}'),
    (409, '{"array_decimal64_1": [11.12]}'),(410, '{"array_decimal64_1": [12.12]}'),(411, '{"array_decimal64_1": [13.12]}'),(412, '{"array_decimal64_1": [14.12]}'),(413, '{"array_decimal64_1": [15.12]}'),(414, '{"array_decimal64_1": [16.12]}'),
    (415, '{"array_decimal64_1": [17.12]}'),(416, '{"array_decimal64_1": [18.12]}'),(417, '{"array_decimal64_1": [19.12]}'),(418, '{"array_decimal64_1": [20.12]}'),(419, '{"array_decimal64_1": [21.12]}'),(420, '{"array_decimal64_1": [22.12]}'),
    (421, '{"array_decimal64_1": [23.12]}'),(422, '{"array_decimal64_1": [24.12]}'),(423, '{"array_decimal64_1": [25.12]}'),(424, '{"array_decimal64_1": [26.12]}'),(425, '{"array_decimal64_1": [27.12]}'),(426, '{"array_decimal64_1": [28.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (427, '{"array_decimal128_1": [1.12]}'),(428, '{"array_decimal128_1": [2.12]}'),(429, '{"array_decimal128_1": [3.12]}'),(430, '{"array_decimal128_1": [4.12]}'),
    (431, '{"array_decimal128_1": [5.12]}'),(432, '{"array_decimal128_1": [6.12]}'),(433, '{"array_decimal128_1": [7.12]}'),(434, '{"array_decimal128_1": [8.12]}'),(435, '{"array_decimal128_1": [9.12]}'),(436, '{"array_decimal128_1": [10.12]}'),
    (437, '{"array_decimal128_1": [11.12]}'),(438, '{"array_decimal128_1": [12.12]}'),(439, '{"array_decimal128_1": [13.12]}'),(440, '{"array_decimal128_1": [14.12]}'),(441, '{"array_decimal128_1": [15.12]}'),(442, '{"array_decimal128_1": [16.12]}'),
    (443, '{"array_decimal128_1": [17.12]}'),(444, '{"array_decimal128_1": [18.12]}'),(445, '{"array_decimal128_1": [19.12]}'),(446, '{"array_decimal128_1": [20.12]}'),(447, '{"array_decimal128_1": [21.12]}'),(448, '{"array_decimal128_1": [22.12]}'),
    (449, '{"array_decimal128_1": [23.12]}'),(450, '{"array_decimal128_1": [24.12]}'),(451, '{"array_decimal128_1": [25.12]}'),(452, '{"array_decimal128_1": [26.12]}'),(453, '{"array_decimal128_1": [27.12]}'),(454, '{"array_decimal128_1": [28.12]}'),
    (455, '{"array_decimal128_1": [29.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (456, '{"array_decimal256_1": [1.12]}'),(457, '{"array_decimal256_1": [2.12]}'),(458, '{"array_decimal256_1": [3.12]}'),(459, '{"array_decimal256_1": [4.12]}'),
    (460, '{"array_decimal256_1": [5.12]}'),(461, '{"array_decimal256_1": [6.12]}'),(462, '{"array_decimal256_1": [7.12]}'),(463, '{"array_decimal256_1": [8.12]}'),(464, '{"array_decimal256_1": [9.12]}'),(465, '{"array_decimal256_1": [10.12]}'),
    (466, '{"array_decimal256_1": [11.12]}'),(467, '{"array_decimal256_1": [12.12]}'),(468, '{"array_decimal256_1": [13.12]}'),(469, '{"array_decimal256_1": [14.12]}'),(470, '{"array_decimal256_1": [15.12]}'),(471, '{"array_decimal256_1": [16.12]}'),
    (472, '{"array_decimal256_1": [17.12]}'),(473, '{"array_decimal256_1": [18.12]}'),(474, '{"array_decimal256_1": [19.12]}'),(475, '{"array_decimal256_1": [20.12]}'),(476, '{"array_decimal256_1": [21.12]}'),(477, '{"array_decimal256_1": [22.12]}'),
    (478, '{"array_decimal256_1": [23.12]}'),(479, '{"array_decimal256_1": [24.12]}'),(480, '{"array_decimal256_1": [25.12]}'),(481, '{"array_decimal256_1": [26.12]}'),(482, '{"array_decimal256_1": [27.12]}'),(483, '{"array_decimal256_1": [28.12]}'),
    (484, '{"array_decimal256_1": [29.12]}'),(485, '{"array_decimal256_1": [30.12]}'); """

    check_table();

    sql """ insert into ${tableName}  values (486, '{"array_datetime_1": ["2021-01-01 00:00:00"]}'),(487, '{"array_datetime_1": ["2021-01-01 00:00:01"]}'),(488, '{"array_datetime_1": ["2021-01-01 00:00:02"]}'),
    (489, '{"array_datetime_1": ["2021-01-01 00:00:03"]}'),(490, '{"array_datetime_1": ["2021-01-01 00:00:04"]}'),(491, '{"array_datetime_1": ["2021-01-01 00:00:05"]}'),(492, '{"array_datetime_1": ["2021-01-01 00:00:06"]}'),
    (493, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(494, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(495, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(496, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'),
    (497, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(498, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(499, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(500, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'),
    (501, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(502, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(503, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(504, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'),
    (505, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(506, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(507, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(508, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'),
    (509, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(510, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(511, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(512, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'),
    (513, '{"array_datetime_1": ["2021-01-01 00:00:07"]}'),(514, '{"array_datetime_1": ["2021-01-01 00:00:08"]}'),(515, '{"array_datetime_1": ["2021-01-01 00:00:09"]}'),(516, '{"array_datetime_1": ["2021-01-01 00:00:10"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (517, '{"array_date_1": ["2021-01-01"]}'),(518, '{"array_date_1": ["2021-01-02"]}'),(519, '{"array_date_1": ["2021-01-03"]}'),(520, '{"array_date_1": ["2021-01-04"]}'),
    (521, '{"array_date_1": ["2021-01-05"]}'),(522, '{"array_date_1": ["2021-01-06"]}'),(523, '{"array_date_1": ["2021-01-07"]}'),(524, '{"array_date_1": ["2021-01-08"]}'),(525, '{"array_date_1": ["2021-01-09"]}'),(526, '{"array_date_1": ["2021-01-10"]}'),
    (527, '{"array_date_1": ["2021-01-07"]}'),(528, '{"array_date_1": ["2021-01-08"]}'),(529, '{"array_date_1": ["2021-01-09"]}'),(530, '{"array_date_1": ["2021-01-10"]}'),
    (531, '{"array_date_1": ["2021-01-07"]}'),(532, '{"array_date_1": ["2021-01-08"]}'),(533, '{"array_date_1": ["2021-01-09"]}'),(534, '{"array_date_1": ["2021-01-10"]}'),
    (535, '{"array_date_1": ["2021-01-07"]}'),(536, '{"array_date_1": ["2021-01-08"]}'),(537, '{"array_date_1": ["2021-01-09"]}'),(538, '{"array_date_1": ["2021-01-10"]}'),
    (539, '{"array_date_1": ["2021-01-07"]}'),(540, '{"array_date_1": ["2021-01-08"]}'),(541, '{"array_date_1": ["2021-01-09"]}'),(542, '{"array_date_1": ["2021-01-10"]}'),
    (543, '{"array_date_1": ["2021-01-07"]}'),(544, '{"array_date_1": ["2021-01-08"]}'),(545, '{"array_date_1": ["2021-01-09"]}'),(546, '{"array_date_1": ["2021-01-10"]}'),
    (547, '{"array_date_1": ["2021-01-07"]}'),(548, '{"array_date_1": ["2021-01-08"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (549, '{"array_ipv4_1": ["192.168.1.1"]}'),(550, '{"array_ipv4_1": ["192.168.1.2"]}'),(551, '{"array_ipv4_1": ["192.168.1.3"]}'),(552, '{"array_ipv4_1": ["192.168.1.4"]}'),
    (553, '{"array_ipv4_1": ["192.168.1.5"]}'),(554, '{"array_ipv4_1": ["192.168.1.6"]}'),(555, '{"array_ipv4_1": ["192.168.1.7"]}'),(556, '{"array_ipv4_1": ["192.168.1.8"]}'),(557, '{"array_ipv4_1": ["192.168.1.9"]}'),(558, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (559, '{"array_ipv4_1": ["192.168.1.7"]}'),(560, '{"array_ipv4_1": ["192.168.1.8"]}'),(561, '{"array_ipv4_1": ["192.168.1.9"]}'),(562, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (563, '{"array_ipv4_1": ["192.168.1.7"]}'),(564, '{"array_ipv4_1": ["192.168.1.8"]}'),(565, '{"array_ipv4_1": ["192.168.1.9"]}'),(566, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (567, '{"array_ipv4_1": ["192.168.1.7"]}'),(568, '{"array_ipv4_1": ["192.168.1.8"]}'),(569, '{"array_ipv4_1": ["192.168.1.9"]}'),(570, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (571, '{"array_ipv4_1": ["192.168.1.7"]}'),(572, '{"array_ipv4_1": ["192.168.1.8"]}'),(573, '{"array_ipv4_1": ["192.168.1.9"]}'),(574, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (575, '{"array_ipv4_1": ["192.168.1.7"]}'),(576, '{"array_ipv4_1": ["192.168.1.8"]}'),(577, '{"array_ipv4_1": ["192.168.1.9"]}'),(578, '{"array_ipv4_1": ["192.168.1.10"]}'),
    (579, '{"array_ipv4_1": ["192.168.1.7"]}'),(580, '{"array_ipv4_1": ["192.168.1.8"]}'),(581, '{"array_ipv4_1": ["192.168.1.9"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (582, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"]}'),(583, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7335"]}'),
    (584, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7336"]}'),(585, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7337"]}'),(586, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7338"]}'),
    (587, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7339"]}'),(588, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733a"]}'),(589, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733b"]}'),
    (590, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733c"]}'),(591, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733d"]}'),(592, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733e"]}'),
    (593, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(594, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(595, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (596, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(597, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(598, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (599, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(600, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(601, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (602, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(603, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(604, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (605, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(606, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(607, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (608, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(609, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(610, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (611, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(612, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'),(613, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}'),
    (614, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}'),(615, '{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}'); """

    check_table();

    sql """ insert into ${tableName}  values (616, '{"other_1": "1"}'),(617, '{"other_1": "2"}'),(618, '{"other_1": "3"}'),(619, '{"other_1": "4"}'),(620, '{"other_1": "5"}'),(621, '{"other_1": "6"}'),(622, '{"other_1": "7"}'),(623, '{"other_1": "8"}'),(624, '{"other_1": "9"}'),(625, '{"other_1": "10"}'),
    (626, '{"other_1": "11"}'),(627, '{"other_1": "12"}'),(628, '{"other_1": "13"}'),(629, '{"other_1": "14"}'),(630, '{"other_1": "15"}'),(631, '{"other_1": "16"}'),(632, '{"other_1": "17"}'),(633, '{"other_1": "18"}'),(634, '{"other_1": "19"}'),(635, '{"other_1": "20"}'),
    (636, '{"other_1": "21"}'),(637, '{"other_1": "22"}'),(638, '{"other_1": "23"}'),(639, '{"other_1": "24"}'),(640, '{"other_1": "25"}'),(641, '{"other_1": "26"}'),(642, '{"other_1": "27"}'),(643, '{"other_1": "28"}'),(644, '{"other_1": "29"}'),(645, '{"other_1": "30"}'),
    (646, '{"other_1": "31"}'),(647, '{"other_1": "32"}'),(648, '{"other_1": "33"}'),(649, '{"other_1": "34"}'),(650, '{"other_1": "35"}'); """

    check_table();
}