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
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `uid` int(11) NULL COMMENT "",
            `c_datetimev2` datetimev2(3) NULL COMMENT "",
            `c_decimal` decimal(8,3) NULL COMMENT "",
            `c_decimalv3` decimalv3(8,3) NULL COMMENT "",
            `c_array_datetimev2` ARRAY<datetimev2(3)> NULL COMMENT "",
            `c_array_decimal` ARRAY<decimal(8,3)> NULL COMMENT ""
            ) ENGINE=OLAP
        DUPLICATE KEY(`uid`)
        DISTRIBUTED BY HASH(`uid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2"
        )
        """

        sql """INSERT INTO ${tableName} values
        (1,"2022-12-01 22:23:24.999999",22.678,33.6789,["2022-12-01 22:23:24.999999","2022-12-01 23:23:24.999999"],[22.678,33.6789]),
        (2,"2022-12-02 22:23:24.999999",23.678,34.6789,["2022-12-02 22:23:24.999999","2022-12-02 23:23:24.999999"],[23.678,34.6789])
        """
        qt_select  "select array_min(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(6))))"
        qt_select  "select array_min(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3))))"
        qt_select  "select array_min(c_array_datetimev2) from ${tableName}"
        qt_select  "select array_max(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(6))))"
        qt_select  "select array_max(array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3))))"
        qt_select  "select array_max(c_array_datetimev2) from ${tableName}"

        qt_select  "select array_min(array(cast (22.99 as decimal)))"
        qt_select  "select array_min(array(cast (22.99 as decimal(10,3))))"
        qt_select  "select array_min(array(cast (22.99 as decimal(10,6))))"
        qt_select  "select array_min(c_array_decimal) from ${tableName}"
        qt_select  "select array_max(array(cast (22.99 as decimal)))"
        qt_select  "select array_max(array(cast (22.99 as decimal(10,3))))"
        qt_select  "select array_max(array(cast (22.99 as decimal(10,6))))"
        qt_select  "select array_max(c_array_decimal) from ${tableName}"

        qt_select  "select array(c_decimal) from ${tableName}"
        qt_select  "select array(cast (24.99 as decimal(10,3)),cast (25.99 as decimal(10,3))) from ${tableName}"
        qt_select  "select array(cast (24.99 as decimal(10,3)),cast (25.99 as decimal(10,3)))"

        qt_select  "select array(c_decimalv3) from ${tableName}"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3))) from ${tableName}"
        qt_select  "select array(cast (24.99 as decimalv3(10,3)),cast (25.99 as decimalv3(10,3)))"

        qt_select "select array(c_datetimev2) from ${tableName}"
        qt_select "select array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.997799' as datetimev2(3))) from ${tableName}"
        qt_select "select array(cast ('2022-12-02 22:23:24.999999' as datetimev2(3)),cast ('2022-12-02 22:23:23.997799' as datetimev2(3)))"

        sql "DROP TABLE IF EXISTS ${tableName}"
}