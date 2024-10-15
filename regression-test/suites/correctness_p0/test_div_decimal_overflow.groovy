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

suite("test_div_decimal_overflow") {
    sql """ drop table if exists table_test_div_decimal_overflow;"""
    sql """CREATE TABLE `table_test_div_decimal_overflow` (
            `total_shy_num` decimalv3(38, 18) NULL,
            `used_shy_num` decimalv3(38, 18) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`total_shy_num`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`total_shy_num`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            );  
            """

    sql """ insert into table_test_div_decimal_overflow values (7166,3242.5);"""

    qt_sql1 """select total_shy_num,used_shy_num,used_shy_num/total_shy_num from table_test_div_decimal_overflow;"""

    sql """ drop table if exists table_test_div_decimal_overflow;"""
}
