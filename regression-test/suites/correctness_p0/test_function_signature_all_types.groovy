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

suite("test_function_signature_all_types", 'nonConcurrent') {
    // Save and set config to allow DATE V1 creation
    def configResult = sql "SHOW FRONTEND CONFIG LIKE 'disable_datev1'"
    logger.info("configResult: ${configResult}")
    assert configResult.size() == 1
    
    def originDisableDatev1 = configResult[0][1]
    logger.info("disable_datev1: $originDisableDatev1")
    
    // Save and set config to control date conversion
    def enableDateConvResult = sql "SHOW FRONTEND CONFIG LIKE 'enable_date_conversion'"
    assert enableDateConvResult.size() == 1
    def originEnableDateConversion = enableDateConvResult[0][1]
    logger.info("enable_date_conversion: $originEnableDateConversion")

    sql "ADMIN SET FRONTEND CONFIG ('disable_datev1' = 'false')"
    logger.info("set disable_datev1 to false")

    sql "ADMIN SET FRONTEND CONFIG ('enable_date_conversion' = 'false')"
    logger.info("set enable_date_conversion to false")
    
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS test_sig_all_types
    """
    
    sql """
        CREATE TABLE test_sig_all_types (
            k1 TINYINT,
            k2 SMALLINT,
            k3 INT,
            k4 BIGINT,
            k5 LARGEINT,
            k6 FLOAT,
            k7 DOUBLE,
            k8 DECIMAL(10, 2),
            k9 CHAR(10),
            k10 VARCHAR(100),
            k11 STRING,
            k12 DATEV1,
            k13 DATETIMEV1,
            k14 DATEV2,
            k15 DATETIMEV2,
            k16 BOOLEAN
        ) DUPLICATE KEY(k1)
        PARTITION BY RANGE(k12) (
            PARTITION p202312 VALUES [('2023-12-01'), ('2024-01-01')),
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
            PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01'))
        )
        DISTRIBUTED BY HASH(k13) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_sig_all_types VALUES
        (0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 'zero', 'zero', 'zero', '2023-12-15', '2023-12-15 00:00:00', '2023-12-15', '2023-12-15 00:00:00', false),
        (1, 10, 100, 1000, 10000, 1.1, 10.01, 100.12, 'char', 'varchar', 'string', '2024-01-05', '2024-01-05 10:00:00', '2024-01-05', '2024-01-05 10:00:00', true),
        (-2, -20, -200, -2000, -20000, -2.2, -20.02, -200.24, 'test', 'test2', 'test3', '2024-02-05', '2024-02-05 11:00:00', '2024-02-05', '2024-02-05 11:00:00', false),
        (3, 30, 300, 3000, 30000, 3.3, 30.03, 300.36, 'abc', 'def', 'ghi', '2024-03-05', '2024-03-05 12:00:00', '2024-03-05', '2024-03-05 12:00:00', true)
    """

    // Verify partition and bucket usage
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-01-05'")
        contains("partitions=1/4")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 BETWEEN '2024-01-05' AND '2024-03-05'")
        contains("partitions=3/4")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-05'")
        contains("partitions=1/4")
    }
    
    // Test single bucket scan with bucket field condition
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-05' AND k13 = '2024-02-05 11:00:00'")
        contains("tablets=1/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k13 = '2024-02-05 11:00:00'")
        contains("partitions=4/4")
        contains("tablets=4/12")
    }

    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-01-05'")
        contains("tablets=3/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 BETWEEN '2024-01-05' AND '2024-03-05'")
        contains("tablets=9/9")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-05'")
        contains("tablets=3/3")
    }

    // Test positive function with all numeric types
    qt_positive_tinyint "SELECT positive(k1) FROM test_sig_all_types ORDER BY k1"
    qt_positive_smallint "SELECT positive(k2) FROM test_sig_all_types ORDER BY k1"
    qt_positive_int "SELECT positive(k3) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT positive(k4) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_positive_bigint "SELECT positive(k4) FROM test_sig_all_types ORDER BY k1"
    
    qt_positive_largeint "SELECT positive(k5) FROM test_sig_all_types ORDER BY k1"
    qt_positive_float "SELECT positive(k6) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT positive(k7) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_positive_double "SELECT positive(k7) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT positive(k8) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_positive_decimal "SELECT positive(k8) FROM test_sig_all_types ORDER BY k1"

    // Test negative function with all numeric types
    qt_negative_tinyint "SELECT negative(k1) FROM test_sig_all_types ORDER BY k1"
    qt_negative_smallint "SELECT negative(k2) FROM test_sig_all_types ORDER BY k1"
    qt_negative_int "SELECT negative(k3) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT negative(k4) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_negative_bigint "SELECT negative(k4) FROM test_sig_all_types ORDER BY k1"
    
    qt_negative_largeint "SELECT negative(k5) FROM test_sig_all_types ORDER BY k1"
    qt_negative_float "SELECT negative(k6) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT negative(k7) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_negative_double "SELECT negative(k7) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT negative(k8) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_negative_decimal "SELECT negative(k8) FROM test_sig_all_types ORDER BY k1"

    // Test abs function with all numeric types
    explain {
        sql("SELECT abs(k1) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_tinyint "SELECT abs(k1) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k2) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_smallint "SELECT abs(k2) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k3) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_int "SELECT abs(k3) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k4) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_bigint "SELECT abs(k4) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k5) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_largeint "SELECT abs(k5) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k6) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_float "SELECT abs(k6) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k7) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_double "SELECT abs(k7) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k8) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_decimal "SELECT abs(k8) FROM test_sig_all_types ORDER BY k1"

    // Test crc32_internal function with all types
    explain {
        sql("SELECT crc32_internal(k1) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_tinyint "SELECT crc32_internal(k1) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k3) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_int "SELECT crc32_internal(k3) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k4) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_bigint "SELECT crc32_internal(k4) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k9) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_char "SELECT crc32_internal(k9) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k10) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_varchar "SELECT crc32_internal(k10) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k11) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_string "SELECT crc32_internal(k11) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k12) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_date "SELECT crc32_internal(k12) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k13) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_datetime "SELECT crc32_internal(k13) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_datev2 "SELECT crc32_internal(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_datetimev2 "SELECT crc32_internal(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT crc32_internal(k1, k3, k10) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_crc32_multi "SELECT crc32_internal(k1, k3, k10) FROM test_sig_all_types ORDER BY k1"

    // Test combined functions and nested calls
    qt_mixed_positive_negative "SELECT positive(k3) + negative(k4) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(abs(k1)) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_abs_nested "SELECT abs(abs(k1)) FROM test_sig_all_types ORDER BY k1"
    
    qt_crc32_with_functions "SELECT crc32_internal(positive(k3), negative(k4)) FROM test_sig_all_types ORDER BY k1"
    qt_mixed_types_1 "SELECT positive(k1), positive(k2), positive(k3), positive(k4), positive(k5), positive(k6), positive(k7), positive(k8) FROM test_sig_all_types ORDER BY k1"
    qt_mixed_types_2 "SELECT negative(k1), negative(k2), negative(k3), negative(k4), negative(k5), negative(k6), negative(k7), negative(k8) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT abs(k1), abs(k2), abs(k3), abs(k4), abs(k5), abs(k6), abs(k7), abs(k8) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_mixed_types_3 "SELECT abs(k1), abs(k2), abs(k3), abs(k4), abs(k5), abs(k6), abs(k7), abs(k8) FROM test_sig_all_types ORDER BY k1"
    
    qt_all_functions_combined "SELECT positive(k3), negative(k4), abs(k1), crc32_internal(k1, k3, k10) FROM test_sig_all_types ORDER BY k1"

    // Test date functions with DATEV2 and DATETIMEV2 signatures
    explain {
        sql("SELECT dayofmonth(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofmonth_datev2 "SELECT dayofmonth(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayofmonth(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofmonth_datetimev2 "SELECT dayofmonth(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayofweek(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofweek_datev2 "SELECT dayofweek(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayofweek(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofweek_datetimev2 "SELECT dayofweek(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayofyear(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofyear_datev2 "SELECT dayofyear(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayofyear(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayofyear_datetimev2 "SELECT dayofyear(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT month(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_month_datev2 "SELECT month(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT month(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_month_datetimev2 "SELECT month(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT year(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_year_datev2 "SELECT year(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT year(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_year_datetimev2 "SELECT year(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT quarter(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_quarter_datev2 "SELECT quarter(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT quarter(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_quarter_datetimev2 "SELECT quarter(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT hour(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_hour_datetimev2 "SELECT hour(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT minute(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_minute_datetimev2 "SELECT minute(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT second(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_second_datetimev2 "SELECT second(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayname(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayname_datev2 "SELECT dayname(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT dayname(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dayname_datetimev2 "SELECT dayname(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT week(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_week_datev2 "SELECT week(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT week(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_week_datetimev2 "SELECT week(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT weekday(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_weekday_datev2 "SELECT weekday(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT weekday(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_weekday_datetimev2 "SELECT weekday(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT weekofyear(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_weekofyear_datev2 "SELECT weekofyear(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT weekofyear(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_weekofyear_datetimev2 "SELECT weekofyear(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT to_date(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_to_date_datetimev2 "SELECT to_date(k15) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT date_format(k14, '%Y-%m-%d') FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dateformat_datev2 "SELECT date_format(k14, '%Y-%m-%d') FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT date_format(k15, '%Y-%m-%d %H:%i:%s') FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_dateformat_datetimev2 "SELECT date_format(k15, '%Y-%m-%d %H:%i:%s') FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT monthname(k14) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_monthname_datev2 "SELECT monthname(k14) FROM test_sig_all_types ORDER BY k1"
    
    explain {
        sql("SELECT monthname(k15) FROM test_sig_all_types")
        notContains("CAST")
    }
    qt_monthname_datetimev2 "SELECT monthname(k15) FROM test_sig_all_types ORDER BY k1"

    // Test combined date functions
    qt_date_functions_combined "SELECT dayofmonth(k14), dayofweek(k14), dayofyear(k14), month(k14), year(k14), quarter(k14) FROM test_sig_all_types ORDER BY k1"
    qt_datetimev2_extract_all "SELECT year(k15), month(k15), dayofmonth(k15), hour(k15), minute(k15), second(k15) FROM test_sig_all_types ORDER BY k1"
    qt_date_mixed_types "SELECT dayofmonth(k14) as day1, dayofmonth(k15) as day2, weekofyear(k14) as week1, weekofyear(k15) as week2 FROM test_sig_all_types ORDER BY k1"
    
    sql """
        DROP TABLE IF EXISTS test_sig_all_types
    """
    
    // ========== Additional DATEV1 Tests ==========
    
    // Test 1: DATEV1 as List Partition key
    sql """
        DROP TABLE IF EXISTS test_datev1_list_partitionv3
    """
    sql """
        CREATE TABLE test_datev1_list_partitionv3 (
            id INT,
            dt DATETIMEV1,
            value VARCHAR(100)
        ) DUPLICATE KEY(id)
        PARTITION BY LIST(dt) (
            PARTITION p1 VALUES IN ('2024-01-15 00:00:00', '2024-01-16 00:00:00', '2024-01-17 00:00:00'),
            PARTITION p2 VALUES IN ('2024-02-15 00:00:00', '2024-02-16 00:00:00', '2024-02-17 00:00:00'),
            PARTITION p3 VALUES IN ('2024-03-15 00:00:00', '2024-03-16 00:00:00', '2024-03-17 00:00:00')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_list_partitionv3 VALUES
        (1, '2024-01-17 00:00:00', 'jan1'),
        (2, '2024-02-16 00:00:00', 'feb1'),
        (3, '2024-03-16 00:00:00', 'mar1')
    """
    qt_datev1_list_partition "SELECT * FROM test_datev1_list_partitionv3 ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_list_partitionv3"
    
    // Test 1.1: DATEV1 as List Partition key (v4)
    sql """
        DROP TABLE IF EXISTS test_datev1_list_partitionv4
    """
    sql """
        CREATE TABLE test_datev1_list_partitionv4 (
            id INT,
            dt DATEV1,
            value VARCHAR(100)
        ) DUPLICATE KEY(id)
        PARTITION BY LIST(dt) (
            PARTITION p1 VALUES IN ('2024-01-15', '2024-01-16', '2024-01-17'),
            PARTITION p2 VALUES IN ('2024-02-15', '2024-02-16', '2024-02-17'),
            PARTITION p3 VALUES IN ('2024-03-15', '2024-03-16', '2024-03-17')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_list_partitionv4 VALUES
        (1, '2024-01-17', 'jan1'),
        (2, '2024-02-16', 'feb1'),
        (3, '2024-03-16', 'mar1')
    """
    qt_datev1_list_partitionv4 "SELECT * FROM test_datev1_list_partitionv4 ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_list_partitionv4"
    
    // Test 2: DATEV1 as bucketing key
    sql """
        DROP TABLE IF EXISTS test_datev1_bucket
    """
    sql """
        CREATE TABLE test_datev1_bucket (
            id INT,
            dt DATEV1,
            value VARCHAR(100)
        ) DUPLICATE KEY(id, dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_bucket VALUES
        (1, '2024-01-15', 'val1'),
        (2, '2024-02-16', 'val2'),
        (3, '2024-03-16', 'val3')
    """
    explain {
        sql("SELECT * FROM test_datev1_bucket WHERE dt = '2024-02-15'")
        contains("tablets=1/3")
    }
    qt_datev1_bucket "SELECT * FROM test_datev1_bucket ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_bucket"
    
    // Test 3: DATEV1 as single column in UNIQUE KEY model
    sql """
        DROP TABLE IF EXISTS test_datev1_single_unique
    """
    sql """
        CREATE TABLE test_datev1_single_unique (
            dt DATEV1,
            value VARCHAR(100)
        ) UNIQUE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_single_unique VALUES
        ('2024-01-15', 'val1'),
        ('2024-02-15', 'val2'),
        ('2024-03-15', 'val3')
    """
    sql """
        INSERT INTO test_datev1_single_unique VALUES
        ('2024-01-15', 'updated_val1')
    """
    qt_datev1_single_unique "SELECT * FROM test_datev1_single_unique ORDER BY dt"
    sql "DROP TABLE IF EXISTS test_datev1_single_unique"
    
    // Test 4: DATEV1 as part of multi-column UNIQUE KEY model
    sql """
        DROP TABLE IF EXISTS test_datev1_multi_unique
    """
    sql """
        CREATE TABLE test_datev1_multi_unique (
            id INT,
            dt DATEV1,
            value VARCHAR(100)
        ) UNIQUE KEY(id, dt)
        DISTRIBUTED BY HASH(id, dt) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_multi_unique VALUES
        (1, '2024-01-15', 'val1'),
        (1, '2024-02-15', 'val2'),
        (2, '2024-01-15', 'val3')
    """
    sql """
        INSERT INTO test_datev1_multi_unique VALUES
        (1, '2024-01-15', 'updated_val1')
    """
    qt_datev1_multi_unique "SELECT * FROM test_datev1_multi_unique ORDER BY id, dt"
    sql "DROP TABLE IF EXISTS test_datev1_multi_unique"
    
    // Test 4.5: DATEV1 as single column in PRIMARY KEY model (Merge-on-Write)
    sql """
        DROP TABLE IF EXISTS test_datev1_single_pk
    """
    sql """
        CREATE TABLE test_datev1_single_pk (
            dt DATEV1 NOT NULL,
            value VARCHAR(100)
        ) UNIQUE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        INSERT INTO test_datev1_single_pk VALUES
        ('2024-01-15', 'val1'),
        ('2024-02-15', 'val2'),
        ('2024-03-15', 'val3')
    """
    sql """
        INSERT INTO test_datev1_single_pk VALUES
        ('2024-01-15', 'updated_val1')
    """
    qt_datev1_single_pk "SELECT * FROM test_datev1_single_pk ORDER BY dt"
    sql "DROP TABLE IF EXISTS test_datev1_single_pk"
    
    // Test 4.6: DATEV1 as part of multi-column PRIMARY KEY model (Merge-on-Write)
    sql """
        DROP TABLE IF EXISTS test_datev1_multi_pk
    """
    sql """
        CREATE TABLE test_datev1_multi_pk (
            id INT NOT NULL,
            dt DATEV1 NOT NULL,
            value VARCHAR(100)
        ) UNIQUE KEY(id, dt)
        DISTRIBUTED BY HASH(id, dt) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        INSERT INTO test_datev1_multi_pk VALUES
        (1, '2024-01-15', 'val1'),
        (1, '2024-02-15', 'val2'),
        (2, '2024-01-15', 'val3')
    """
    sql """
        INSERT INTO test_datev1_multi_pk VALUES
        (1, '2024-01-15', 'updated_val1')
    """
    qt_datev1_multi_pk "SELECT * FROM test_datev1_multi_pk ORDER BY id, dt"
    sql "DROP TABLE IF EXISTS test_datev1_multi_pk"
    
    // Test 5: DATEV1 as single AGG key
    sql """
        DROP TABLE IF EXISTS test_datev1_single_agg
    """
    sql """
        CREATE TABLE test_datev1_single_agg (
            dt DATEV1,
            value INT SUM
        ) AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_single_agg VALUES
        ('2024-01-15', 10),
        ('2024-01-15', 20),
        ('2024-02-15', 30)
    """
    qt_datev1_single_agg "SELECT * FROM test_datev1_single_agg ORDER BY dt"
    sql "DROP TABLE IF EXISTS test_datev1_single_agg"
    
    // Test 6: DATEV1 as part of multi-column AGG key
    sql """
        DROP TABLE IF EXISTS test_datev1_multi_agg
    """
    sql """
        CREATE TABLE test_datev1_multi_agg (
            id INT,
            dt DATEV1,
            value INT SUM
        ) AGGREGATE KEY(id, dt)
        DISTRIBUTED BY HASH(id, dt) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_multi_agg VALUES
        (1, '2024-01-15', 10),
        (1, '2024-01-15', 20),
        (1, '2024-02-15', 30),
        (2, '2024-01-15', 40)
    """
    qt_datev1_multi_agg "SELECT * FROM test_datev1_multi_agg ORDER BY id, dt"
    sql "DROP TABLE IF EXISTS test_datev1_multi_agg"
    
    // Test 7: DATEV1 as join key with STRING, check common type
    sql """
        DROP TABLE IF EXISTS test_datev1_join_left
    """
    sql """
        DROP TABLE IF EXISTS test_datev1_join_right
    """
    sql """
        CREATE TABLE test_datev1_join_left (
            id INT,
            dt DATEV1,
            value VARCHAR(100)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_datev1_join_right (
            id INT,
            dt_str STRING,
            info VARCHAR(100)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_join_left VALUES
        (1, '2024-01-15', 'left1'),
        (2, '2024-02-15', 'left2'),
        (3, '2024-03-15', 'left3')
    """
    sql """
        INSERT INTO test_datev1_join_right VALUES
        (1, '2024-01-15', 'right1'),
        (2, '2024-02-15', 'right2'),
        (4, '2024-04-15', 'right4')
    """
    qt_datev1_join "SELECT l.id, l.dt, l.value, r.info FROM test_datev1_join_left l JOIN test_datev1_join_right r ON l.dt = r.dt_str ORDER BY l.id"
    sql "DROP TABLE IF EXISTS test_datev1_join_left"
    sql "DROP TABLE IF EXISTS test_datev1_join_right"
    
    // Test 8: DATEV1 in predicates for filtering and pruning
    sql """
        DROP TABLE IF EXISTS test_datev1_predicate
    """
    sql """
        CREATE TABLE test_datev1_predicate (
            id INT,
            dt DATEV1,
            value VARCHAR(100)
        ) DUPLICATE KEY(id)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p2 VALUES [('2024-02-01'), ('2024-03-01')),
            PARTITION p3 VALUES [('2024-03-01'), ('2024-04-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_predicate VALUES
        (1, '2024-01-15', 'val1'),
        (2, '2024-02-15', 'val2'),
        (3, '2024-03-15', 'val3')
    """
    // Test equal predicate
    explain {
        sql("SELECT * FROM test_datev1_predicate WHERE dt = '2024-02-15'")
        contains("partitions=1/3")
    }
    // Test range predicate
    explain {
        sql("SELECT * FROM test_datev1_predicate WHERE dt >= '2024-02-01' AND dt < '2024-03-01'")
        contains("partitions=1/3")
    }
    // Test IN predicate
    explain {
        sql("SELECT * FROM test_datev1_predicate WHERE dt IN ('2024-01-15', '2024-03-15')")
        contains("partitions=2/3")
    }
    qt_datev1_predicate_eq "SELECT * FROM test_datev1_predicate WHERE dt = '2024-02-15' ORDER BY id"
    qt_datev1_predicate_range "SELECT * FROM test_datev1_predicate WHERE dt >= '2024-02-01' AND dt < '2024-03-01' ORDER BY id"
    qt_datev1_predicate_in "SELECT * FROM test_datev1_predicate WHERE dt IN ('2024-01-15', '2024-03-15') ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_predicate"
    
    // Test 9: DATEV1 in complex types (ARRAY with DATEV1 elements)
    sql """
        DROP TABLE IF EXISTS test_datev1_array
    """
    sql """
        CREATE TABLE test_datev1_array (
            id INT,
            date_arr ARRAY<DATEV1>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_array VALUES
        (1, ['2024-01-01', '2024-01-02', '2024-01-03']),
        (2, ['2024-02-01', '2024-02-02']),
        (3, ['2024-03-01'])
    """
    qt_datev1_array "SELECT id, array_size(date_arr) as size, date_arr FROM test_datev1_array ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_array"
    
    // Test 10: DATEV1 in MAP type
    sql """
        DROP TABLE IF EXISTS test_datev1_map
    """
    sql """
        CREATE TABLE test_datev1_map (
            id INT,
            date_map MAP<STRING, DATEV1>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_map VALUES
        (1, {'start': '2024-01-01', 'end': '2024-01-31'}),
        (2, {'start': '2024-02-01', 'end': '2024-02-29'}),
        (3, {'start': '2024-03-01', 'end': '2024-03-31'})
    """
    qt_datev1_map "SELECT id, map_size(date_map) as size FROM test_datev1_map ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_map"
    
    // Test 12: DATEV1 with BloomFilter index and pruning signal
    sql """
        DROP TABLE IF EXISTS test_datev1_bloom
    """
    sql """
        CREATE TABLE test_datev1_bloom (
            id INT,
            dt DATEV1,
            v  INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(dt) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "dt"
        )
    """
    sql """
        INSERT INTO test_datev1_bloom VALUES
        (1, '2024-01-15', 10),
        (2, '2024-01-16', 20),
        (3, '2024-02-15', 30)
    """
    explain {
        sql("SELECT * FROM test_datev1_bloom WHERE dt = '2024-01-15'")
        contains("tablets=1/3")
    }
    qt_datev1_bloom "SELECT * FROM test_datev1_bloom WHERE dt = '2024-01-15' ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_bloom"

    // Test 13: DATEV1 delete condition (DELETE FROM)
    sql """
        DROP TABLE IF EXISTS test_datev1_delete
    """
    sql """
        CREATE TABLE test_datev1_delete (
            id INT,
            dt DATEV1,
            v  INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_delete VALUES
        (1, '2024-01-15', 10),
        (2, '2024-02-15', 20),
        (3, '2024-03-15', 30)
    """
    sql "DELETE FROM test_datev1_delete WHERE dt < '2024-02-01'"
    qt_datev1_delete_after "SELECT * FROM test_datev1_delete ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_delete"

    // Test 14: DATEV1 default CURRENT_TIMESTAMP (implicit cast to date)
    sql """
        DROP TABLE IF EXISTS test_datev1_default
    """
    sql """
        CREATE TABLE test_datev1_default (
            id INT,
            dt DATETIMEV1 DEFAULT CURRENT_TIMESTAMP,
            v  INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_datev1_default(id, v) VALUES (1, 10)"
    sql "INSERT INTO test_datev1_default VALUES (2, '2024-02-15', 20)"
    qt_datev1_default "SELECT id, (dt >= '2000-01-01') as ok, v FROM test_datev1_default ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_default"

    // Test 15: Compute layer - group by/order by/distinct/join
    sql """
        DROP TABLE IF EXISTS test_datev1_compute
    """
    sql """
        CREATE TABLE test_datev1_compute (
            id INT,
            dt DATEV1,
            v  INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_compute VALUES
        (1, '2024-01-15', 10),
        (2, '2024-01-15', 20),
        (3, '2024-02-15', 30),
        (4, '2024-03-15', 40)
    """
    qt_datev1_group "SELECT dt, sum(v) FROM test_datev1_compute GROUP BY dt ORDER BY dt"
    qt_datev1_order "SELECT id, dt FROM test_datev1_compute ORDER BY dt, id"
    qt_datev1_distinct "SELECT DISTINCT dt FROM test_datev1_compute ORDER BY dt"
    qt_datev1_join2 "SELECT a.id, b.id, a.dt FROM test_datev1_compute a JOIN test_datev1_compute b ON a.dt = b.dt WHERE a.id <> b.id ORDER BY a.id, b.id"
    sql "DROP TABLE IF EXISTS test_datev1_compute"

    // Test 16: Casts and implicit casts with DATEV1
    sql """
        DROP TABLE IF EXISTS test_datev1_cast
    """
    sql """
        CREATE TABLE test_datev1_cast (
            id INT,
            dt DATEV1
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_cast VALUES
        (1, '2024-01-15'),
        (2, '2024-02-16')
    """
    qt_datev1_cast_explicit "SELECT id, cast(dt as DATETIMEV1) FROM test_datev1_cast ORDER BY id"
    qt_datev1_cast_implicit_cmp "SELECT count(*) FROM test_datev1_cast WHERE dt < '2024-02-01 00:00:00'"
    qt_datev1_to_string "SELECT id, cast(dt as string) FROM test_datev1_cast ORDER BY id"
    sql "DROP TABLE IF EXISTS test_datev1_cast"

    // Test 18: Runtime filter on DATEV1 join key
    sql """
        DROP TABLE IF EXISTS test_datev1_rf_fact
    """
    sql """
        DROP TABLE IF EXISTS test_datev1_rf_dim
    """
    sql """
        CREATE TABLE test_datev1_rf_fact (
            id INT,
            dt DATEV1,
            v  INT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 6
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_datev1_rf_dim (
            d DATEV1
        ) DUPLICATE KEY(d)
        DISTRIBUTED BY HASH(d) BUCKETS 6
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datev1_rf_fact VALUES
        (1, '2024-01-15', 10), (2, '2024-02-15', 20), (3, '2024-03-15', 30)
    """
    sql """
        INSERT INTO test_datev1_rf_dim VALUES
        ('2024-01-15'), ('2024-03-15')
    """
    
    qt_datev1_rf_join "SELECT f.id, f.dt, f.v FROM test_datev1_rf_fact f JOIN test_datev1_rf_dim d ON f.dt = d.d ORDER BY f.id"
    sql "DROP TABLE IF EXISTS test_datev1_rf_fact"
    sql "DROP TABLE IF EXISTS test_datev1_rf_dim"

    // ========== Constant expression checks: math & date functions (DATEV1/DATETIMEV1) ==========
    // Math
    testFoldConst("SELECT abs(-3), positive(-2), negative(3)")
    testFoldConst("SELECT ceil(1.2), floor(1.8), round(1.25, 1)")
    testFoldConst("SELECT sqrt(4), power(2, 10), mod(7, 3)")
    testFoldConst("SELECT ln(1), log10(100), exp(1)")
    testFoldConst("SELECT greatest(1, 2, 3), least(1, 2, 3)")
    testFoldConst("SELECT sin(0), cos(0), tan(0)")
    testFoldConst("SELECT radians(180), degrees(pi())")

    // Date (v1 semantics) — cast literals to DATEV1/DATETIMEV1
    testFoldConst("SELECT dayofmonth(cast('2024-02-05' as DATEV1)), " +
                  "dayofweek(cast('2024-02-05' as DATEV1)), " +
                  "dayofyear(cast('2024-02-05' as DATEV1))")
    testFoldConst("SELECT month(cast('2024-02-05' as DATEV1)), " +
                  "year(cast('2024-02-05' as DATEV1)), " +
                  "quarter(cast('2024-02-05' as DATEV1))")
    testFoldConst("SELECT hour(cast('2024-02-05 12:34:56' as DATETIMEV1)), " +
                  "minute(cast('2024-02-05 12:34:56' as DATETIMEV1)), " +
                  "second(cast('2024-02-05 12:34:56' as DATETIMEV1))")
    testFoldConst("SELECT dayname(cast('2024-02-05' as DATEV1)), " +
                  "week(cast('2024-02-05' as DATEV1)), " +
                  "weekday(cast('2024-02-05' as DATEV1)), " +
                  "weekofyear(cast('2024-02-05' as DATEV1))")
    testFoldConst("SELECT date_format(cast('2024-02-05' as DATEV1), '%Y-%m-%d'), " +
                  "to_date(cast('2024-02-05 12:00:00' as DATETIMEV1))")
    testFoldConst("SELECT date_trunc('day', cast('2024-02-05 12:34:56' as DATETIMEV1)), " +
                  "date_trunc('month', cast('2024-02-05 12:34:56' as DATETIMEV1))")

    // Restore config
    sql "ADMIN SET FRONTEND CONFIG ('disable_datev1' = '${originDisableDatev1}')"
    logger.info("restore disable_datev1 to ${originDisableDatev1}")
    sql "ADMIN SET FRONTEND CONFIG ('enable_date_conversion' = '${originEnableDateConversion}')"
    logger.info("restore enable_date_conversion to ${originEnableDateConversion}")
}
