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

suite ("test_mv_mow") {
    sql """ drop table if exists u_table; """

    sql """
            create table u_table (
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            unique key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
    sql "insert into u_table select 1,1,1,1;"
    sql "insert into u_table select 1,2,1,1;"
    createMV("create materialized view k123p as select k1,k2+k3 from u_table;")

    sql "insert into u_table select 1,1,1,1;"
    sql "insert into u_table select 1,2,1,1;"

    explain {
        sql("select k1,k2+k3 from u_table order by k1;")
        contains "(k123p)"
    }
    qt_select_mv "select k1,k2+k3 from u_table order by k1;"
}
