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

suite("test_order_by_mod") {
    sql """set enable_nereids_planner=true"""
    sql """
        drop table if exists order_by_mod_t;
    """

    sql """
        create table order_by_mod_t(a int, b float, c int) 
        duplicate key(a)
        distributed by hash(a) buckets 1
        properties ("replication_num" = "1");
    """

    sql """
        insert into order_by_mod_t values (10, 3.3, 3), (20, 3.3, 3);;
    """
    qt_select_mod """
        select * from order_by_mod_t order by mod(a,c) limit 1;;
    """
    qt_select_op """
        select * from order_by_mod_t order by a % c limit 1;
    """
}