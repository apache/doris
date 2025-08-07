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

suite("avg_weighted") {
    sql "set enable_decimal256 = true;"
    sql """
        drop table if exists d_table;
    """
    
    sql """
    create table d_table (
        k1 int null,
        k2 int not null,
        k3 bigint null,
        k4 varchar(100) null,
        col_double double null,
        weight_double double null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    
    // 插入测试数据
    sql """
    insert into d_table values 
        (1, 1, 1, 'test1', 
         2.718281828, 1.0)
    """
    
    qt_sql_double """select avg_weighted(col_double, weight_double) from d_table;"""
}