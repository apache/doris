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

suite("big_count_distinct") {
    sql "drop table if EXISTS b_table"
    sql """
       create table b_table (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        """

    sql """insert into b_table select e1,e1,e1,'a' from (select 1 k1) as t lateral view explode_numbers(250000000) tmp1 as e1;"""
    set "set enable_local_exchange=false;"
    set "set parallel_pipeline_task_num=1;"
    test {
        sql "SELECT count(distinct cast(k1 as varchar)) FROM b_table group by k4;"
        exception "LZ4_compressBound meet invalid input size"
    }
}

