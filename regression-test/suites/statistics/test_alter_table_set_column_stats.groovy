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

suite("test_alter_table_set_column_stats") {

    sql """drop database if exists test_alter_table_set_column_stats"""
    sql """create database test_alter_table_set_column_stats"""
    sql """use test_alter_table_set_column_stats"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""
    sql """drop table if exists et"""
    sql """
        CREATE TABLE `et` (
        `QTY` DECIMAL(18, 2) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`QTY`)
        COMMENT 'st_entry_detail_et'
        DISTRIBUTED BY HASH(`QTY`) BUCKETS 40
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """alter table et modify column QTY set stats ('row_count'='1690909875', 'ndv'='836', 'min_value'='-893077', 'max_value'='987118080', 'avg_size'='288004948', 'max_size'='288004948' );"""
    def result = sql """show column stats et"""
    assertEquals(1, result.size())
    assertEquals("QTY", result[0][0])
    assertEquals("et", result[0][1])
    assertEquals("1.690909875E9", result[0][2])
    assertEquals("836.0", result[0][3])
    assertEquals("0.0", result[0][4])
    assertEquals("0.0", result[0][5])
    assertEquals("0.0", result[0][6])
    assertEquals("-893077", result[0][7])
    assertEquals("987118080", result[0][8])

    test {
        sql """alter table et modify column qty set stats ('row_count'='1690909875', 'ndv'='836', 'min_value'='-893077', 'max_value'='987118080', 'avg_size'='288004948', 'max_size'='288004948' );"""
        exception "Unknown column 'qty' in 'et'"
    }
    sql """drop database if exists test_alter_table_set_column_stats"""
}

