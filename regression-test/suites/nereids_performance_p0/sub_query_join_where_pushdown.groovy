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

suite("sub_query_join_where_pushdown") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        DROP TABLE IF EXISTS sub_query_join_where_pushdown1
        """

    sql """
        DROP TABLE IF EXISTS sub_query_join_where_pushdown2
        """

    sql """
        DROP TABLE IF EXISTS sub_query_join_where_pushdown3
        """

    sql """CREATE TABLE `sub_query_join_where_pushdown1` (
          `id` int NULL,
          `day` date NULL,
          `hour` int NULL,
          `code` string NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""

    sql """CREATE TABLE `sub_query_join_where_pushdown2` (
          `day` date NULL,
          `hour` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`day`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`day`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""

    sql """CREATE TABLE `sub_query_join_where_pushdown3` (
          `id` int NULL,
          `day` date NULL,
          `hour` int NULL,
          `code` string NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""

    sql """insert into sub_query_join_where_pushdown1 values (1,'2023-01-01',1,'big'),(2,'2023-01-01',2,'big');"""
    sql """insert into sub_query_join_where_pushdown2 values ('2023-01-01',3),('2023-01-01',4);"""
    sql """insert into sub_query_join_where_pushdown3 values (1,'2023-01-01',1,'big'),(2,'2023-01-01',2,'big');"""

    explain {
        sql ("""with tmp as
                (
                	select a.day,a.code,a.hour,count(*)
                	from
                	(
                		select day,code,hour,count(*)
                		from sub_query_join_where_pushdown1
                		group by 1,2,3
                	) a
                	right join (select day,hour from sub_query_join_where_pushdown2) b
                	on b.hour>=a.hour and b.day=a.day
                	where a.day is not null
                	group by 1,2,3
                )
                select * from tmp where code = 'big' and day = '2023-01-01';""")
        contains "PREDICATES: code[#5] = 'big', day[#3] = '2023-01-01', day[#3] IS NOT NULL"
        contains "PREDICATES: day[#0] = '2023-01-01'"
    }

    explain {
        sql ("""select dd.*
                from
                        (select day,hour,code,count(*)
                       from sub_query_join_where_pushdown1
                       group by 1,2,3
                       ) final
                JOIN
                       ( select day,hour,code,count(*)
                        from sub_query_join_where_pushdown3
                        group by 1,2,3
                        ) dd
                on dd.hour=final.`hour` and dd.`day`=final.`day` and dd.code=final.code
                where dd.code = 'big';""")
        contains "PREDICATES: code[#14] = 'big'"
        contains "PREDICATES: code[#3] = 'big'"
    }
}