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

suite("test_mv_case") {
    sql """drop table if exists test_table_aaa2;"""
    sql """CREATE TABLE `test_table_aaa2` (
            `ordernum` varchar(65533) NOT NULL ,
            `dnt` datetime NOT NULL ,
            `data` json NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`ordernum`, `dnt`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ods_zn_dnt_max1 ON test_table_aaa2;"""
    createMV("""create materialized view ods_zn_dnt_max1 as
            select ordernum,max(dnt) as dnt from test_table_aaa2
            group by ordernum
            ORDER BY ordernum;""")
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:55:33','{"DB1":168939,"DNT":"2023-06-10 03:55:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:56:33','{"DB1":168939,"DNT":"2023-06-10 03:56:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:57:33','{"DB1":168939,"DNT":"2023-06-10 03:57:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:58:33','{"DB1":168939,"DNT":"2023-06-10 03:58:33"}' ;"""
    qt_select_default """ select * from test_table_aaa2 order by dnt;"""

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

    sql """ drop table if exists tb1 """
    sql """ CREATE TABLE tb1 (
        `id` bigint NOT NULL COMMENT '',
        `map_infos` map < int,
        varchar(65533) > NULL COMMENT ''
        ) ENGINE = OLAP UNIQUE KEY(`id`) COMMENT 'test' DISTRIBUTED BY HASH(`id`) BUCKETS 2 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_mow_light_delete" = "false"
        )
    """
    sql """insert into tb1 select id,map_agg(a, b) from(select 123 id,3 a,'5' b union all select 123 id, 6 a, '8' b) aa group by id"""
    createMV ("""CREATE MATERIALIZED VIEW mv1 BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 MINUTE DUPLICATE KEY(info_id) DISTRIBUTED BY HASH(`info_id`) BUCKETS 2 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "colocate_with" = "dwd_info_group",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_nondeterministic_function" = "true"
        ) AS
        select
        /*+ SET_VAR(enable_force_spill = true) */
        cast(a.id as bigint) info_id,
        map_infos
        from
        tb1 a;""")
    createMV ("""CREATE MATERIALIZED VIEW mv2 BUILD IMMEDIATE REFRESH COMPLETE ON SCHEDULE EVERY 10 MINUTE DUPLICATE KEY(info_id) DISTRIBUTED BY HASH(`info_id`) BUCKETS 2 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "colocate_with" = "dwd_info_group",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_nondeterministic_function" = "true"
        ) AS
        select
        info_id,
        map_infos
        from
        mv1 a;""")
    qt_select_mv """ select * from mv2 """
}
