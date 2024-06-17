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

suite("intersect_nullable_not_nullable") {
    sql """
    drop table if exists intersect_nullable_not_nullable_t1; 
    """
    sql """
    drop table if exists intersect_nullable_not_nullable_t2; 
    """
    sql """
    drop table if exists intersect_nullable_not_nullable_t3; 
    """
    sql """
    drop table if exists intersect_nullable_not_nullable_t4; 
    """
    sql """
    create table intersect_nullable_not_nullable_t1 (k1 char(16) not null) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
    insert into intersect_nullable_not_nullable_t1 values("a"), ("b"), ("c"), ("d"), ("e");
    """
    
    sql """
    create table intersect_nullable_not_nullable_t2 (kk0 int, kk1 char(16) not null) distributed by hash(kk0) properties("replication_num"="1");
    """
    sql """
    insert into intersect_nullable_not_nullable_t2 values(1, "b"), (2, "c"), (3, "d"), (4, "e");
    """
    
    sql """
    create table intersect_nullable_not_nullable_t3 (kkk1 char(16) ) distributed by hash(kkk1) properties("replication_num"="1");
    """
    sql """
    insert into intersect_nullable_not_nullable_t3 values("c"), ("d"), ("e");
    """
    
    sql """
    create table intersect_nullable_not_nullable_t4 (kkkk1 char(16) ) distributed by hash(kkkk1) properties("replication_num"="1");
    """
    sql """
    insert into intersect_nullable_not_nullable_t4 values("d"), ("e");
    """
    
    order_qt_intersect_nullable_not_nullable_1 """
        (
            select * from intersect_nullable_not_nullable_t1
        )
        intersect
        (
            select distinct kk1 from intersect_nullable_not_nullable_t2
        )
        intersect
        (
            (
                select * from intersect_nullable_not_nullable_t3
            )
            except
            (
                select * from intersect_nullable_not_nullable_t4
            )
        );
    """
    
    order_qt_intersect_nullable_not_nullable_2 """
        (
            select * from intersect_nullable_not_nullable_t1
        )
        intersect
        (
            (
                select * from intersect_nullable_not_nullable_t3
            )
            except
            (
                select * from intersect_nullable_not_nullable_t4
            )
        )
        intersect
        (
            select distinct kk1 from intersect_nullable_not_nullable_t2
        );
    """
}
