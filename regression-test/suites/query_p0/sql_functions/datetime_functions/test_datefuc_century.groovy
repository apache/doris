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

suite("test_century_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tableName = "test_century_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id int,
            test_date date NULL,
            test_datetime datetime NULL,
            test_datetimev2 datetimev2(3) NULL,
            test_date_str varchar(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, '1989-03-21', '1989-03-21 10:30:45', '1989-03-21 10:30:45.123', '1989-03-21'),
        (2, '2000-01-01', '2000-01-01 00:00:00', '2000-01-01 00:00:00.000', '2000-01-01'),
        (3, '2001-01-01', '2001-01-01 12:30:45', '2001-01-01 12:30:45.456', '2001-01-01'),
        (4, '2015-07-14', '2015-07-14 15:45:30', '2015-07-14 15:45:30.789', '2015-07-14'),
        (5, '1900-12-31', '1900-12-31 23:59:59', '1900-12-31 23:59:59.999', '1900-12-31'),
        (6, '1901-01-01', '1901-01-01 00:00:00', '1901-01-01 00:00:00.000', '1901-01-01'),
        (7, '1999-12-31', '1999-12-31 23:59:59', '1999-12-31 23:59:59.999', '1999-12-31'),
        (8, '2001-01-01', '2001-01-01 00:00:00', '2001-01-01 00:00:00.000', '2001-01-01'),
        (9, '0001-01-01', '0001-01-01 00:00:00', '0001-01-01 00:00:00.000', '0001-01-01'),
        (10, '0100-12-31', '0100-12-31 23:59:59', '0100-12-31 23:59:59.999', '0100-12-31'),
        (11, '0101-01-01', '0101-01-01 00:00:00', '0101-01-01 00:00:00.000', '0101-01-01'),
        (12, '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59.999', '9999-12-31'),
        (13, '2024-01-01', '2024-01-01 10:00:00', '2024-01-01 10:00:00.123', '2024-01-01'),
        (14, NULL, NULL, NULL, NULL)
    """

    qt_century_basic """
        SELECT 
            century('1989-03-21') as century_1989,
            century('2000-01-01') as century_2000,
            century('2001-01-01') as century_2001,
            century('2015-07-14') as century_2015
    """

    qt_century_boundary """
        SELECT
            century('1900-12-31') as century_1900_end,
            century('1901-01-01') as century_1901_start,
            century('1999-12-31') as century_1999_end,
            century('2000-01-01') as century_2000_start,
            century('2000-12-31') as century_2000_end,
            century('2001-01-01') as century_2001_start
    """

    qt_century_extreme """
        SELECT
            century('0001-01-01') as century_1,
            century('0100-12-31') as century_1_end,
            century('0101-01-01') as century_2_start,
            century('9999-12-31') as century_100,
            century('1000-01-01') as century_10,
            century('1500-06-15') as century_15
    """

    qt_century_null """
        SELECT century(NULL) as century_null
    """

    qt_century_formats """
        SELECT
            century('2023-07-14') as date_only,
            century('2023-07-14 15:30:45') as with_time,
            century('1999-12-31') as y2k_date,
            century('2000-01-01 00:00:00') as y2k_timestamp
    """

    qt_century_table_data """
        SELECT 
            id,
            test_date,
            century(test_date) as century_from_date,
            century(test_datetime) as century_from_datetime,
            century(test_datetimev2) as century_from_datetimev2,
            century(test_date_str) as century_from_string
        FROM ${tableName}
        WHERE id <= 13
        ORDER BY id
    """

    qt_century_batch """
        SELECT 
            date_str,
            century(date_str) as calculated_century
        FROM (
            SELECT '1989-03-21' as date_str
            UNION ALL SELECT '2000-01-01'
            UNION ALL SELECT '2001-01-01' 
            UNION ALL SELECT '1901-01-01'
            UNION ALL SELECT '1900-12-31'
            UNION ALL SELECT '0101-01-01'
            UNION ALL SELECT '0001-01-01'
            UNION ALL SELECT '9999-12-31'
            UNION ALL SELECT '2024-01-01'
            UNION ALL SELECT '1492-10-12'
            UNION ALL SELECT '1776-07-04'
            UNION ALL SELECT '1945-08-15'
        ) dates
        ORDER BY date_str
    """

    qt_century_invalid_dates """
        SELECT 
            century('invalid-date') as invalid_date,
            century('2023-02-30') as invalid_day,
            century('2023-13-01') as invalid_month,
            century('') as empty_string
    """

    qt_century_with_year """
        SELECT 
            test_date,
            year(test_date) as year,
            century(test_date) as century,
            (year(test_date) - 1) / 100 + 1 as calculated_century
        FROM ${tableName}
        WHERE test_date IS NOT NULL
        ORDER BY test_date
    """

    sql "set parallel_pipeline_task_num = 8"
    
    qt_century_vec1 """
        SELECT
            id, test_date, century(test_date)
        FROM ${tableName}
        WHERE test_date IS NOT NULL
        ORDER BY id
    """

    qt_century_vec2 """
        SELECT
            id, test_datetime, century(test_datetime)
        FROM ${tableName}
        WHERE test_datetime IS NOT NULL
        ORDER BY id
    """

    explain {
        sql """SELECT * FROM ${tableName} WHERE century(test_date) = 21"""
        contains "century"
    }

    explain {
        sql """SELECT * FROM ${tableName} WHERE century(test_datetime) > 20"""
        contains "century"
    }

    sql """ DROP TABLE IF EXISTS test_century_performance """
    sql """
        CREATE TABLE test_century_performance (
            id int,
            event_date date
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO test_century_performance
        SELECT 
            number as id,
            DATE_ADD('2000-01-01', INTERVAL (number % 10000) DAY) as event_date
        FROM numbers("number" = "10000")
    """

    qt_century_performance """
        SELECT 
            century(event_date) as event_century,
            COUNT(*) as count
        FROM test_century_performance
        GROUP BY century(event_date)
        ORDER BY event_century
    """

    qt_century_special_cases """
        SELECT
            century('0001-01-01') as first_century,
            century('0100-12-31') as first_century_end,
            century('0101-01-01') as second_century_start,
            century('1000-12-31') as tenth_century_end,
            century('1001-01-01') as eleventh_century_start,
            century('2000-12-31') as twentieth_century_end,
            century('2001-01-01') as twenty_first_century_start
    """

    qt_century_combined_functions """
        SELECT
            test_date,
            century(test_date) as orig_century,
            century(DATE_ADD(test_date, INTERVAL 100 YEAR)) as plus_100_years,
            century(DATE_SUB(test_date, INTERVAL 100 YEAR)) as minus_100_years
        FROM ${tableName}
        WHERE test_date IS NOT NULL AND id <= 5
        ORDER BY id
    """

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ DROP TABLE IF EXISTS test_century_performance """
}
