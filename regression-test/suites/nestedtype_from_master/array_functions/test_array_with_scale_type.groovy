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

suite("test_array_with_scale_type") {

    def tableName = "test_array_with_scale_type_table"
        sql "DROP TABLE IF EXISTS test_array_with_scale_type_table"
        sql """
            CREATE TABLE IF NOT EXISTS `test_array_with_scale_type_table` (
            `uid` int(11) NULL COMMENT "",
            `c_datetimev2` datetimev2(3) NULL COMMENT "",
            `c_decimal` decimal(8,3) NULL COMMENT "",
            `c_decimalv3` decimalv3(8,3) NULL COMMENT "",
            `c_array_datetimev2` ARRAY<datetimev2(3)> NULL COMMENT "",
            `c_array_decimal` ARRAY<decimal(8,3)> NULL COMMENT "",
            `c_array_decimalv3` ARRAY<decimalv3(8,3)> NULL COMMENT ""
            ) ENGINE=OLAP
        DUPLICATE KEY(`uid`)
        DISTRIBUTED BY HASH(`uid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
        """

        // load with same insert into data
        streamLoad {
            table "test_array_with_scale_type_table"

            set 'column_separator', '|'

            file 'test_array_with_scale_type.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2, json.NumberTotalRows)
                assertEquals(2, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
            }
        }
        sql """
        INSERT INTO test_array_with_scale_type_table values
        (1,"2022-12-01 22:23:24.999999",22.6789,33.6789,["2022-12-01 22:23:24.999999","2022-12-01 23:23:24.999999"],[22.6789,33.6789],[22.6789,33.6789]),
        (2,"2022-12-02 22:23:24.999999",23.6789,34.6789,["2022-12-02 22:23:24.999999","2022-12-02 23:23:24.999999"],[23.6789,34.6789],[22.6789,34.6789]);
        """
        qt_select_varify_datetime """
            SELECT c_datetimev2 from test_array_with_scale_type_table order by uid;
        """
        qt_select  "select array_min(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(6))))"
        qt_select  "select array_min(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3))))"
        qt_select  "select array_min(c_array_datetimev2) from test_array_with_scale_type_table order by uid"
        qt_select  "select array_max(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(6))))"
        qt_select  "select array_max(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3))))"
        qt_select  "select array_max(c_array_datetimev2) from test_array_with_scale_type_table order by uid"

        qt_select  "select array_min(array(cast (22.99 as decimalv3)))"
        qt_select  "select array_min(array(cast (22.99 as decimalv3(10,3))))"
        qt_select  "select array_min(array(cast (22.99 as decimalv3(10,6))))"
        qt_select  "select array_min(c_array_decimal) from test_array_with_scale_type_table order by uid"
        qt_select  "select array_min(c_array_decimalv3) from test_array_with_scale_type_table order by uid"
        qt_select  "select array_max(array(cast (22.99 as decimalv3)))"
        qt_select  "select array_max(array(cast (22.99 as decimalv3(10,3))))"
        qt_select  "select array_max(array(cast (22.99 as decimalv3(10,6))))"
        qt_select  "select array_max(c_array_decimal) from test_array_with_scale_type_table order by uid"
        qt_select  "select array_max(c_array_decimalv3) from test_array_with_scale_type_table order by uid"

        qt_select  "select array(c_decimal) from test_array_with_scale_type_table order by uid"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3))) from test_array_with_scale_type_table order by uid"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3)))"

        qt_select  "select array(c_decimalv3) from test_array_with_scale_type_table order by uid"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3))) from test_array_with_scale_type_table order by uid"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3)))"

        qt_select "select array(c_datetimev2) from test_array_with_scale_type_table order by uid"
        qt_select "select array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.997799' as datetimev2(3))) from test_array_with_scale_type_table order by uid"
        qt_select "select array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.997799' as datetimev2(3)))"

        qt_select """select array_apply(c_array_datetimev2, "=", '2022-12-02 22:23:24.999999') from test_array_with_scale_type_table order by uid"""
        qt_select """select array_apply(c_array_datetimev2, ">", '2022-12-01 22:23:24.999999') from test_array_with_scale_type_table order by uid"""
        qt_select """select array_apply(c_array_datetimev2, ">", null) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_apply(c_array_decimal, "=", 22.679) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_apply(c_array_decimal, ">=", 22.1) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_apply(c_array_decimal, ">=", null) from test_array_with_scale_type_table order by uid"""

        qt_select """select array_concat(array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3)),cast ('2022-12-02 22:23:23.123123' as datetimev2(3)))) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_concat(c_array_datetimev2) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_concat(c_array_datetimev2, array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3)),cast ('2022-12-02 22:23:23.123123' as datetimev2(3)))) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_concat(c_array_decimal, c_array_decimal, c_array_decimal) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_zip(c_array_decimal, c_array_decimal, c_array_datetimev2, c_array_decimal) from test_array_with_scale_type_table order by uid"""

        qt_select """select array_zip(array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3)),cast ('2022-12-02 22:23:23.123123' as datetimev2(3)))) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_zip(c_array_datetimev2) from test_array_with_scale_type_table order by uid"""
        qt_select """select array_zip(c_array_datetimev2, array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3)),cast ('2022-12-02 22:23:23.123123' as datetimev2(3)))) from test_array_with_scale_type_table order by uid"""

        qt_select "select array_pushfront(array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3))),cast ('2022-12-02 22:23:23.123123' as datetimev2(3))) from test_array_with_scale_type_table order by uid"
        qt_select "select array_pushfront(c_array_datetimev2, cast ('2023-03-08 23:23:23.123123' as datetimev2(3))) from test_array_with_scale_type_table order by uid"
        qt_select "select c_datetimev2, c_array_datetimev2, array_pushfront(c_array_datetimev2, c_datetimev2) from test_array_with_scale_type_table order by c_datetimev2, uid"
        qt_select "select array_pushfront(c_array_decimal, cast (25.99 as decimalv3(10,3))) from test_array_with_scale_type_table order by uid"
        qt_select "select c_decimal, c_array_decimal, array_pushfront(c_array_decimal, c_decimal) from test_array_with_scale_type_table order by c_decimal, uid"

        qt_select "select array_pushback(array(cast ('2022-12-02 22:23:24.123123' as datetimev2(3))),cast ('2022-12-02 22:23:23.123123' as datetimev2(3))) from test_array_with_scale_type_table"
        qt_select "select array_pushback(c_array_datetimev2, cast ('2023-03-08 23:23:23.123123' as datetimev2(3))) from test_array_with_scale_type_table"
        qt_select "select c_datetimev2, c_array_datetimev2, array_pushback(c_array_datetimev2, c_datetimev2) from test_array_with_scale_type_table order by c_datetimev2, uid"
        qt_select "select array_pushback(c_array_decimal, cast (25.99 as decimalv3(10,3))) from test_array_with_scale_type_table order by uid"
        qt_select "select c_decimal, c_array_decimal, array_pushback(c_array_decimal, c_decimal) from test_array_with_scale_type_table order by c_decimal, uid"
        
        qt_select  "select array_cum_sum(array(cast (22.99 as decimal), cast (-11.99 as decimal)))"
        qt_select  "select array_cum_sum(array(cast (22.99 as decimal(10,3)), cast (-11.99 as decimal(10,3))))"
        qt_select  "select array_cum_sum(array(cast (22.99 as decimal(10,6)), cast (-11.991 as decimal(10,6))))"
        qt_select  "select array_cum_sum(c_array_decimal) from test_array_with_scale_type_table order by uid"
}
