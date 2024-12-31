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

suite("eliminate_order_by_constant") {
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "drop table if exists eliminate_order_by_constant_t"
    sql """create table eliminate_order_by_constant_t(a int null, b int not null, c varchar(10) null, d date, dt datetime)
    distributed by hash(a) properties("replication_num"="1");
    """
    sql """
    INSERT INTO eliminate_order_by_constant_t (a, b, c, d, dt) VALUES
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (1, 100, 'apple', '2023-01-01', '2023-01-01 10:00:00'),
    (2, 101, 'banana', '2023-01-02', '2023-01-02 11:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'),
    (3, 102, 'cherry', '2023-01-03', '2023-01-03 12:00:00'), 
    (NULL, 103, 'date', '2023-01-04', '2023-01-04 13:00:00'),
    (4, 104, 'elderberry', '2023-01-05', '2023-01-05 14:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (5, 105, NULL, '2023-01-06', '2023-01-06 15:00:00'),
    (6, 106, 'fig', '2023-01-07', '2023-01-07 16:00:00'),
    (NULL, 107, 'grape', '2023-01-08', '2023-01-08 17:00:00');
    """
    qt_predicate "select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a"
    qt_predicate_order_by_two "select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a,c1"
    qt_with_group_by """select 1 as c1,a from eliminate_order_by_constant_t where a=1 group by c1,a order by a"""
    qt_predicate_multi_other """select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',b,c"""
    qt_predicate_shape "explain shape plan select 1 as c1,a from eliminate_order_by_constant_t where a=1 order by a"
    qt_with_group_by_shape """explain shape plan select 1 as c1,a from eliminate_order_by_constant_t where a=1 group by c1,a order by a"""
    qt_predicate_multi_other_shape """explain shape plan select 1 as c1,a,b,c from eliminate_order_by_constant_t where a=1 order by a,'abc',b,c"""
}