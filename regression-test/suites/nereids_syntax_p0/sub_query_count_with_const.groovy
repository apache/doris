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

suite("sub_query_count_with_const") {
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS sub_query_count_with_const
        """

    sql """CREATE TABLE `sub_query_count_with_const` (
             `id` int(11) NULL
           ) ENGINE=OLAP
         DUPLICATE KEY(`id`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`id`) BUCKETS 1
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
         );"""

    sql """insert into sub_query_count_with_const values(1),(2),(3);"""

    qt_select """select count(1) as count
                 from (
                       select 2022 as dt ,sum(id)
                       from sub_query_count_with_const
                 ) tmp;"""

    explain {
        sql ("""select count(1) as count
                from (
                      select 2022 as dt ,sum(id)
                      from sub_query_count_with_const
                ) tmp;""")
        contains "output: sum(id[#0])[#1]"
    }
}