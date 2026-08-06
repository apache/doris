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
suite("costbasedrewrite_producer") {
    sql """
    drop table if exists t1;

    create table t1(a1 int,b1 int)
    properties("replication_num" = "1");

    insert into t1 values(1,2);

    drop table if exists t2;

    create table t2(a2 int,b2 int)
    properties("replication_num" = "1");

    insert into t2 values(1,3);
    """

    sql"""
   with cte1 as (
    select t1.a1, t1.b1
    from t1
    where t1.a1 > 0 and not exists (select distinct t2.b2 from t2 where t1.a1 = t2.a2 or t1.b1 = t2.a2)  
    ),
    cte2 as (
    select * from cte1 union  select * from cte1)
   select * from cte2 join t1 on cte2.a1 = t1.a1;

    """
}
