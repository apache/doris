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

suite("test_function_signature_all_types") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // 创建测试表,包含各种数据类型
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
            k12 DATE,
            k13 DATETIME,
            k14 DATEV2,
            k15 DATETIMEV2,
            k16 BOOLEAN
        ) DUPLICATE KEY(k1)
        PARTITION BY RANGE(k12) (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
            PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01'))
        )
        DISTRIBUTED BY HASH(k13) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // 插入测试数据
    sql """
        INSERT INTO test_sig_all_types VALUES
        (1, 10, 100, 1000, 10000, 1.1, 10.01, 100.12, 'char', 'varchar', 'string', '2024-01-01', '2024-01-01 10:00:00', '2024-01-01', '2024-01-01 10:00:00', true),
        (-2, -20, -200, -2000, -20000, -2.2, -20.02, -200.24, 'test', 'test2', 'test3', '2024-02-01', '2024-02-01 11:00:00', '2024-02-01', '2024-02-01 11:00:00', false),
        (3, 30, 300, 3000, 30000, 3.3, 30.03, 300.36, 'abc', 'def', 'ghi', '2024-03-01', '2024-03-01 12:00:00', '2024-03-01', '2024-03-01 12:00:00', true)
    """

    // Verify partition and bucket usage
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-01-01'")
        contains("partitions=1/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 BETWEEN '2024-01-01' AND '2024-03-01'")
        contains("partitions=3/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-01'")
        contains("partitions=1/3")
    }
    
    // Test single bucket scan with bucket field condition
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-01' AND k13 = '2024-02-01 11:00:00'")
        contains("tablets=1/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k13 = '2024-02-01 11:00:00'")
        contains("partitions=3/3")
        contains("tablets=3/9")
    }

    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-01-01'")
        contains("tablets=3/3")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 BETWEEN '2024-01-01' AND '2024-03-01'")
        contains("tablets=9/9")
    }
    
    explain {
        sql("SELECT * FROM test_sig_all_types WHERE k12 = '2024-02-01'")
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
}
