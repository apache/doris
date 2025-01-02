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

suite("push_down_aggr_distinct_through_join_one_side_cust") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set DISABLE_NEREIDS_RULES='PRUNE_EMPTY_PARTITION, ELIMINATE_GROUP_BY_KEY_BY_UNIFORM'"

    sql """
        DROP TABLE IF EXISTS dwd_com_abtest_result_inc_ymds;
	DROP TABLE IF EXISTS dwd_tracking_sensor_init_tmp_ymds;
    """

    sql """
    CREATE TABLE `dwd_com_abtest_result_inc_ymds` (
      `app_name` varchar(255) NULL,
      `user_key` text NULL,
      `group_name` text NULL,
      `dt` date NOT NULL,
    ) ENGINE=OLAP
    DUPLICATE KEY(`app_name`)
    AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
    (PARTITION p20240813000000 VALUES [('2024-08-13'), ('2024-08-14')),
    PARTITION p20240814000000 VALUES [('2024-08-14'), ('2024-08-15')),
    PARTITION p20240815000000 VALUES [('2024-08-15'), ('2024-08-16')),
    PARTITION p20240816000000 VALUES [('2024-08-16'), ('2024-08-17')),
    PARTITION p20240817000000 VALUES [('2024-08-17'), ('2024-08-18')),
    PARTITION p20240818000000 VALUES [('2024-08-18'), ('2024-08-19')),
    PARTITION p20240819000000 VALUES [('2024-08-19'), ('2024-08-20')))
    DISTRIBUTED BY HASH(`app_name`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "inverted_index_storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
    );

    CREATE TABLE `dwd_tracking_sensor_init_tmp_ymds` (
      `ip` varchar(20) NULL,
      `gz_user_id` text NULL,
      `dt` date NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ip`)
    AUTO PARTITION BY RANGE (date_trunc(`dt`, 'day'))
    (PARTITION p20240813000000 VALUES [('2024-08-13'), ('2024-08-14')),
    PARTITION p20240814000000 VALUES [('2024-08-14'), ('2024-08-15')),
    PARTITION p20240815000000 VALUES [('2024-08-15'), ('2024-08-16')),
    PARTITION p20240816000000 VALUES [('2024-08-16'), ('2024-08-17')),
    PARTITION p20240817000000 VALUES [('2024-08-17'), ('2024-08-18')),
    PARTITION p20240818000000 VALUES [('2024-08-18'), ('2024-08-19')),
    PARTITION p20240819000000 VALUES [('2024-08-19'), ('2024-08-20')))
    DISTRIBUTED BY HASH(`ip`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "inverted_index_storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
    );
    """

    explain {
        sql("physical PLAN SELECT /*+use_cbo_rule(PUSH_DOWN_AGG_WITH_DISTINCT_THROUGH_JOIN_ONE_SIDE)*/" +
                "COUNT(DISTINCT dwd_tracking_sensor_init_tmp_ymds.gz_user_id) AS a2c1a830_1," +
                "dwd_com_abtest_result_inc_ymds.group_name AS ab1011d6," +
                "dwd_tracking_sensor_init_tmp_ymds.dt AS ad466123 " +
                "FROM dwd_tracking_sensor_init_tmp_ymds " +
                "LEFT JOIN dwd_com_abtest_result_inc_ymds " +
                "ON dwd_tracking_sensor_init_tmp_ymds.gz_user_id = dwd_com_abtest_result_inc_ymds.user_key " +
                "AND dwd_tracking_sensor_init_tmp_ymds.dt = dwd_com_abtest_result_inc_ymds.dt " +
                "WHERE dwd_tracking_sensor_init_tmp_ymds.dt BETWEEN '2024-08-15' AND '2024-08-15' " +
                "AND dwd_com_abtest_result_inc_ymds.dt BETWEEN '2024-08-15' AND '2024-08-15' " +
                "GROUP BY 2, 3 ORDER BY 3 asc limit 10000;");
        contains"groupByExpr=[gz_user_id#1, dt#2]"
        contains"groupByExpr=[gz_user_id#1, dt#2, group_name#5], outputExpr=[gz_user_id#1, dt#2, group_name#5]"
        contains"[group_name#5, dt#2]"
        contains"groupByExpr=[group_name#5, dt#2], outputExpr=[group_name#5, dt#2, count(partial_count(gz_user_id)#12) AS `a2c1a830_1`#7]"
    }

    explain {
        sql("physical PLAN SELECT /*+use_cbo_rule(PUSH_DOWN_AGG_WITH_DISTINCT_THROUGH_JOIN_ONE_SIDE)*/" +
                "COUNT(DISTINCT dwd_tracking_sensor_init_tmp_ymds.ip) AS a2c1a830_1," +
                "dwd_com_abtest_result_inc_ymds.group_name AS ab1011d6," +
                "dwd_tracking_sensor_init_tmp_ymds.dt AS ad466123 " +
                "FROM dwd_tracking_sensor_init_tmp_ymds " +
                "LEFT JOIN dwd_com_abtest_result_inc_ymds " +
                "ON dwd_tracking_sensor_init_tmp_ymds.gz_user_id = dwd_com_abtest_result_inc_ymds.user_key " +
                "AND dwd_tracking_sensor_init_tmp_ymds.dt = dwd_com_abtest_result_inc_ymds.dt " +
                "WHERE dwd_tracking_sensor_init_tmp_ymds.dt BETWEEN '2024-08-15' AND '2024-08-15' " +
                "AND dwd_com_abtest_result_inc_ymds.dt BETWEEN '2024-08-15' AND '2024-08-15' " +
                "GROUP BY 2, 3 ORDER BY 3 asc limit 10000;");
        contains"groupByExpr=[ip#0, gz_user_id#1, dt#2], outputExpr=[ip#0, gz_user_id#1, dt#2]"
        contains"groupByExpr=[ip#0, dt#2, group_name#5], outputExpr=[ip#0, dt#2, group_name#5]"
        contains"groupByExpr=[group_name#5, dt#2], outputExpr=[group_name#5, dt#2, partial_count(ip#0) AS `partial_count(ip)`#12]"
        contains"groupByExpr=[group_name#5, dt#2], outputExpr=[group_name#5, dt#2, count(partial_count(ip)#12) AS `a2c1a830_1`#7]"
    }
}
