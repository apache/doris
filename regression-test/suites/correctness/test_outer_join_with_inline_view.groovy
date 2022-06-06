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

suite("test_outer_join_with_inline_view") {
    sql """
        drop table if exists ojwiv_t1;
    """

    sql """
        drop table if exists ojwiv_t2;
    """
    
    sql """
        create table if not exists ojwiv_t1(
          k1 int not null,
          v1 int not null
        )
        distributed by hash(k1)
        properties(
          'replication_num' = '1'
        );
    """

    sql """
        create table if not exists ojwiv_t2(
          k1 int not null,
          c1 varchar(255) not null
        )
        distributed by hash(k1)
        properties('replication_num' = '1');
    """

    sql """
        insert into ojwiv_t1 values(1, 1), (2, 2), (3, 3), (4, 4);
    """

    sql """
        insert into ojwiv_t2 values(1, '1'), (2, '2');
    """

    qt_select_with_order_by """
        select * from
          (select * from ojwiv_t1) a
        left outer join
          (select * from ojwiv_t2) b
        on a.k1 = b.k1
        order by a.v1; 
    """

    qt_select_with_agg_in_inline_view """
        select * from
          (select * from ojwiv_t1) a
        left outer join
          (select k1, count(distinct c1) from ojwiv_t2 group by k1) b
        on a.k1 = b.k1
        order by a.v1; 
    """
}
