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
suite("percentile_array") {
    sql "set enable_decimal256 = true;"
    sql """
        drop table if exists d_table;
    """

    sql """
    create table d_table (
        k1 int null,
        k2 int not null,
        k3 bigint null,
        col_tinyint tinyint null,
        col_smallint smallint null,
        col_int int null,
        col_bigint bigint null,
        col_largeint largeint null,
        col_float float null,
        col_double double null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """

    // 插入测试数据
    sql """
    insert into d_table values 
        (1, 1, 1, 100, 10000, 1000000, 10000000000, 100000000000000000000, 3.14, 2.718281828),
        (2, 2, 2, 101, 10001, 1000001, 10000000001, 100000000000000000001, 6.28, 3.141592653),
        (3, 3, 3, 102, 10002, 1000002, 10000000002, 100000000000000000002, 9.42, 1.618033988)
    """

    // 测试percentile_array聚合函数支持的类型
    qt_percentile_array_tinyint """select percentile_array(col_tinyint, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_smallint """select percentile_array(col_smallint, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_int """select percentile_array(col_int, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_bigint """select percentile_array(col_bigint, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_largeint """select percentile_array(col_largeint, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_float """select percentile_array(col_float, array(0.5, 0.8)) from d_table;"""
    qt_percentile_array_double """select percentile_array(col_double, array(0.5, 0.8)) from d_table;"""
}