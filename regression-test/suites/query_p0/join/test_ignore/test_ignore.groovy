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

suite("test_ignore") {
    sql """ DROP TABLE IF EXISTS u_table; """
    sql """ DROP TABLE IF EXISTS u_table2; """
    sql """
        create table u_table (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        properties("replication_num" = "1");
        """

    sql "insert into u_table select e1,e1,e1,'a' from (select 1 k1) as t lateral view explode_numbers(1000000) tmp1 as e1;"

    sql """
        create table u_table2 (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1)
        properties("replication_num" = "1");
        """

    sql "insert into u_table2 select e1,e1,e1,'a' from (select 1 k1) as t lateral view explode_numbers(100000) tmp1 as e1;"

    sql "set enable_runtime_filter_prune=false;"
    sql "set parallel_pipeline_task_num=1;"

    qt_test """select count(*) from u_table a, u_table2 b where a.k3=b.k3;"""
    qt_test """select count(*) from u_table a, u_table2 b where a.k3=b.k3 and a.k3!=6666 and b.k3!=6666;"""
}
