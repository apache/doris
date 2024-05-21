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

suite("test_rowstore", "p0") {
    def tableName = "rs_query"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql "set enable_decimal256 = true"
    sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` DECIMAL(50, 18) NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
          """

    sql "set experimental_enable_nereids_planner = false"
    sql """insert into ${tableName} values (1, 'abc', 1111919.12345678919)"""
    explain {
        sql("select * from ${tableName}")
        contains "OPT TWO PHASE"
    } 
    qt_sql """select * from ${tableName}"""

    sql """
         ALTER table ${tableName} ADD COLUMN new_column1 INT default "123";
    """
    qt_sql """select * from ${tableName} where k1 = 1"""

    sql """
         ALTER table ${tableName} ADD COLUMN new_column2 DATETIMEV2(3) DEFAULT "1970-01-01 00:00:00.111";
    """
    sleep(1000)
    qt_sql """select * from ${tableName} where k1 = 1"""
   
    sql """insert into ${tableName} values (2, 'def', 1111919.12345678919, 456, NULL)"""
    qt_sql """select * from ${tableName} where k1 = 2"""
}