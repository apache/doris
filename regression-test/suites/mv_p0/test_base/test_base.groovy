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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_base") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ drop table if exists dwd;"""

    sql """
        CREATE TABLE `dwd` (
            `id` bigint(20) NULL COMMENT 'id',
            `created_at` datetime NULL,
            `dt` date NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """insert into dwd(id) values(1);"""

    createMV ("""
            create materialized view dwd_mv as SELECT created_at, id FROM dwd;
    """)

    sql """insert into dwd(id) values(2);"""

    explain {
        sql("SELECT created_at, id FROM dwd order by 1, 2;")
        contains "(dwd_mv)"
    }
    qt_select_mv "SELECT created_at, id FROM dwd order by 1, 2;"

    explain {
        sql("SELECT id,created_at  FROM dwd order by 1, 2;")
        contains "(dwd)"
    }
    qt_select_mv "SELECT id,created_at FROM dwd order by 1, 2;"

    sql """set enable_nereids_planner=false;"""
    sql """drop table if exists test_mv_view_t;"""
    sql """drop view if exists test_mv_view_t_view;"""
    sql """CREATE TABLE `test_mv_view_t` (
            `day` date NOT NULL,
            `game_code` varchar(100) NOT NULL ,
            `plat_code` varchar(100) NOT NULL
            ) ENGINE=OLAP
            duplicate KEY(`day`)
            DISTRIBUTED BY HASH(`day`) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """INSERT INTO test_mv_view_t VALUES('2024-04-01',  'x', 'y');"""
    createMV ("""create  materialized view  test_mv_view_t_mv as
                select `day`, count(game_code)
                from test_mv_view_t group by day;""")
    sql """create view test_mv_view_t_view 
            as
            select `day`
                from test_mv_view_t
                where day<'2024-04-15'

            union all
                select `day`
                from test_mv_view_t
                where day>='2024-04-15';"""
    explain {
        sql("""SELECT  * from test_mv_view_t_view where day='2024-04-15';""")
        notContains("mv_day")
    }
}
