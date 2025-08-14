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

suite("str64_serialize") {
    
    sql """ DROP TABLE IF EXISTS d_table; """
    sql """ DROP TABLE IF EXISTS d_table2; """


    sql """
            create table d_table (
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """
    sql """
            create table d_table2 (
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql """insert into d_table select 1,1,1,'1234567890abcdefghigalsdhaluihdicandejionxaoxwdeuhwenudzmwoedxneiowdxiowedjxneiowdjixoneiiexdnuiexef' from (select 1 k1) as t lateral view explode_numbers(50000000) tmp1 as e1;
"""

    sql """insert into d_table2 select 1,1,1,'1234567890abcdefghigalsdhaluihdicandejionxaoxwdeuhwenudzmwoedxneiowdxiowedjxneiowdjixoneiiexdnuiexef';
"""
    sql "set parallel_pipeline_task_num=1;"

    qt_test "select /*+ LEADING(a,b) */ count(*) from d_table as a, d_table2 as b where a.k4=b.k4 and a.k1=b.k1;"

    // this query will use too much memory.
    // qt_test "select /*+ LEADING(b,a) */ count(*) from d_table as a, d_table2 as b where a.k4=b.k4 and a.k1=b.k1;"
}

