
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

suite("test_join_with_const", "query,p0") {
    def left_table = "left_table"
    def right_table = "right_table"
    sql " drop table if exists ${left_table}; ";
    sql " drop table if exists ${right_table}; ";
    sql """
        create table ${left_table} (c1 datev2, c2 bigint sum) 
        aggregate key (c1) 
        DISTRIBUTED BY HASH(c1) 
        BUCKETS 3 
        properties ("replication_num" = "1");
    """
    sql """
        create table ${right_table} (c1 datev2, c2 bigint sum) 
        aggregate key (c1) 
        DISTRIBUTED BY HASH(c1) 
        BUCKETS 3 
        properties ("replication_num" = "1");
    """

    sql """ insert into ${left_table} values ("2024-10-31", 1), ("2024-11-01", 1); """
    sql """ insert into ${right_table} values ("2024-11-01", 2); """

    def join_sql_str = """ 
    select
        *
    from
        (
            select 0 z, c1, sum(c2) c2
            from ${left_table}
            group by 1, 2
        ) t1 
        FULL JOIN [shuffle] 
        (
            select 0 z, c1, sum(c2) c2
            from ${right_table}
            group by 1, 2
        ) t2 
        on t1.z = t2.z
        and t1.c1 = t2.c1
    order by t1.c1
    """

    sql "set enable_nereids_planner = false;"
    qt_sql1 "${join_sql_str}"

    sql "set enable_nereids_planner = true;"
    qt_sql1 "${join_sql_str}"

    sql "drop table ${left_table}"
    sql "drop table ${right_table}"

}