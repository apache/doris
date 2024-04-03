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

suite("test_decimalv2_calc", "nonConcurrent") {

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """
    sql "set check_overflow_for_decimal=false;"

    def table1 = "test_decimalv2_calc_tbl"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
        `decimal_key1` decimalv2(8, 5) NULL COMMENT "",
        `decimal_key2` decimalv2(16, 5) NULL COMMENT "",
        `decimal_value1` decimalv2(8, 5) NULL COMMENT "",
        `decimal_value2` decimalv2(16, 5) NULL COMMENT ""
      ) ENGINE=OLAP
      DUPLICATE KEY(`decimal_key1`, `decimal_key2`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`decimal_key1`, `decimal_key2`) BUCKETS 4
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

    sql """insert into ${table1} values
            (1.1,   1.4,    1.5,    1.6),
            (1.1,   1.2,    1.56,   1.234),
            (1.2,   1.2,    1.66,   1.534654),
            (1.2,   1.2,    1.344,  1.32432),
            (1.3,   1.2,    1.456,  1.345435),
            (2.1,   1.2,    1.3,    1.234),
            (2.1,   1.2,    1.3,    1.2342),
            (2.2,   1.4,    1.21,   1.123),
            (NULL,  NULL,   NULL,   NULL)
    """
    qt_select_all "select * from ${table1} order by 1, 2, 3, 4"

    qt_select_calc1 """
        select
            decimal_key1, decimal_key1
            , decimal_key1 + decimal_key1 v1
            , decimal_key1 - decimal_key1 v2
            , decimal_key1 * decimal_key1 v3
            , decimal_key1 / decimal_key1 v4
        from ${table1} order by 1, 2, 3, 4, 5;
    """

    qt_select_calc2 """
        select
            decimal_key1
            , decimal_key2
            , decimal_key1 + decimal_key2 v1
            , decimal_key1 - decimal_key2 v2
            , decimal_key1 * decimal_key2 v3
            , decimal_key1 / decimal_key2 v4
            , decimal_key2 + decimal_key1 v5
            , decimal_key2 - decimal_key1 v6
            , decimal_key2 * decimal_key1 v7
            , decimal_key2 / decimal_key1 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc3 """
        select
            decimal_key1
            , decimal_value1
            , decimal_key1 + decimal_value1 v1
            , decimal_key1 - decimal_value1 v2
            , decimal_key1 * decimal_value1 v3
            , decimal_key1 / decimal_value1 v4
            , decimal_value1 + decimal_key1 v5
            , decimal_value1 - decimal_key1 v6
            , decimal_value1 * decimal_key1 v7
            , decimal_value1 / decimal_key1 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc4 """
        select
            decimal_key1
            , decimal_value2
            , decimal_key1 + decimal_value2 v1
            , decimal_key1 - decimal_value2 v2
            , decimal_key1 * decimal_value2 v3
            , decimal_key1 / decimal_value2 v4
            , decimal_value2 + decimal_key1 v5
            , decimal_value2 - decimal_key1 v6
            , decimal_value2 * decimal_key1 v7
            , decimal_value2 / decimal_key1 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc5 """
        select
            decimal_key2
            , decimal_key2 + decimal_key2 v1
            , decimal_key2 - decimal_key2 v2
            , decimal_key2 * decimal_key2 v3
            , decimal_key2 / decimal_key2 v4
        from ${table1} order by 1, 2, 3, 4, 5;
    """

    qt_select_calc6 """
        select
            decimal_key2
            , decimal_value1
            , decimal_key2 + decimal_value1 v1
            , decimal_key2 - decimal_value1 v2
            , decimal_key2 * decimal_value1 v3
            , decimal_key2 / decimal_value2 v4
            , decimal_value1 + decimal_key2 v5
            , decimal_value1 - decimal_key2 v6
            , decimal_value1 * decimal_key2 v7
            , decimal_value1 / decimal_key2 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc7 """
        select
            decimal_key2
            , decimal_value2
            , decimal_key2 + decimal_value2 v1
            , decimal_key2 - decimal_value2 v2
            , decimal_key2 * decimal_value2 v3
            , decimal_key2 / decimal_value2 v4
            , decimal_value2 + decimal_key2 v5
            , decimal_value2 - decimal_key2 v6
            , decimal_value2 * decimal_key2 v7
            , decimal_value2 / decimal_key2 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc8 """
        select
            decimal_value1
            , decimal_value1 + decimal_value1 v1
            , decimal_value1 - decimal_value1 v2
            , decimal_value1 * decimal_value1 v3
            , decimal_value1 / decimal_value1 v4
        from ${table1} order by 1, 2, 3, 4, 5;
    """

    qt_select_calc9 """
        select
            decimal_value1
            , decimal_value2
            , decimal_value1 + decimal_value2 v1
            , decimal_value1 - decimal_value2 v2
            , decimal_value1 * decimal_value2 v3
            , decimal_value1 / decimal_value2 v4
            , decimal_value2 + decimal_value1 v5
            , decimal_value2 - decimal_value1 v6
            , decimal_value2 * decimal_value1 v7
            , decimal_value2 / decimal_value1 v8
        from ${table1} order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;
    """

    qt_select_calc10 """
        select
            decimal_value2
            , decimal_value2 + decimal_value2 v1
            , decimal_value2 - decimal_value2 v2
            , decimal_value2 * decimal_value2 v3
            , decimal_value2 / decimal_value2 v4
        from ${table1} order by 1, 2, 3, 4, 5;
    """

    qt_calc_overflow """
        select
            decimal_value1
            , decimal_value2
            , decimal_value1 * 999.99999 v1
            , decimal_value2 * 0.9999999 v1
        from ${table1} order by 1, 2, 3, 4;
    """

    sql """
        admin set frontend config("enable_decimal_conversion" = "true");
    """
}
