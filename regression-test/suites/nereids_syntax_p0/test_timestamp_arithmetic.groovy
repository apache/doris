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

suite("nereids_timestamp_arithmetic") {

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql = "select bitmap_empty() + interval 1 year;"
        exception = "Unexpected exception: Operand 'bitmap_empty()' of timestamp arithmetic expression 'years_add(bitmap_empty(), INTERVAL 1 YEAR)' returns type 'BITMAP'. Expected type 'TIMESTAMP/DATE/DATETIME'"
    }

    test {
        sql = "select date '20200808' + interval array() day;"
        exception = "the second argument must be a scalar type. but it is ARRAY()"
    }

    sql """
    DROP TABLE IF EXISTS nereids_test_ta;
    """
    sql """
        CREATE TABLE `nereids_test_ta` (
          `c1` int(11) NULL,
          `c2` date NULL,
          `c3` datev2 NULL,
          `c4` datetime NULL,
          `c5` datetimev2(3) NULL,
          `c6` datetimev2(5) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c1`, `c2`, `c3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        INSERT INTO nereids_test_ta VALUES (1, '0001-01-01', '0001-01-01', '0001-01-01 00:01:01', '0001-01-01: 00:01:01.001', '0001-01-01 00:01:01.00305');
    """

    qt_test_add """
        SELECT c2 + INTERVAL 1 DAY, c3 + INTERVAL 1 SECOND, c4 + INTERVAL 1 DAY, c5 + INTERVAL 1 DAY, c6 + INTERVAL 1 DAY FROM nereids_test_ta
    """
}