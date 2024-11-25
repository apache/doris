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

suite("lateral_view") {
    sql """ DROP TABLE IF EXISTS `test_explode_bitmap` """
	sql """
		CREATE TABLE `test_explode_bitmap` (
		  `dt` int(11) NULL COMMENT "",
		  `page` varchar(10) NULL COMMENT "",
		  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
		) ENGINE=OLAP
		AGGREGATE KEY(`dt`, `page`)
		DISTRIBUTED BY HASH(`dt`) BUCKETS 2 
		properties("replication_num"="1");
	"""
	sql """ insert into test_explode_bitmap values(1, '11', bitmap_from_string("1,2,3"));"""
	sql """ insert into test_explode_bitmap values(2, '22', bitmap_from_string("22,33,44"));"""

    sql "SET enable_nereids_planner=false"
	qt_sql_explode_bitmap0 """ select dt, e1 from test_explode_bitmap lateral view explode_bitmap(user_id) tmp1 as e1 order by dt, e1;"""

    sql """ DROP TABLE IF EXISTS `entity_37_label_summary` """
    sql """
        CREATE TABLE `entity_37_label_summary` (
          `label_id` int(11) NOT NULL COMMENT '标签标识',
          `update_time` datetime NOT NULL COMMENT '更新时间',
          `label_value` varchar(128) NULL COMMENT '标签值',
          `label_type` int(11) NULL COMMENT '标签类型',
          `conf_score` DECIMAL(10, 2) NULL COMMENT '可信度分',
          `act_score` DECIMAL(10, 2) NULL COMMENT 'ID 活跃度',
          `source_dt` datetime NULL,
          `seq_id_bitmap` bitmap BITMAP_UNION NOT NULL COMMENT 'SeqID集合'
        ) ENGINE=OLAP
        AGGREGATE KEY(`label_id`, `update_time`, `label_value`, `label_type`, `conf_score`, `act_score`, `source_dt`)
        COMMENT '标签汇总表'
        PARTITION BY RANGE(`update_time`)
        (PARTITION p20220512 VALUES [('0000-01-01 00:00:00'), ('2022-05-13 00:00:00')),
        PARTITION p20220513 VALUES [('2022-05-13 00:00:00'), ('2022-05-14 00:00:00')),
        PARTITION p20241124 VALUES [('2024-11-24 00:00:00'), ('2024-11-25 00:00:00')))
        DISTRIBUTED BY HASH(`label_id`, `label_value`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.time_zone" = "Asia/Shanghai",
        "dynamic_partition.start" = "-2147483648",
        "dynamic_partition.end" = "2",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.buckets" = "3",
        "dynamic_partition.create_history_partition" = "false",
        "dynamic_partition.history_partition_num" = "-1",
        "dynamic_partition.hot_partition_num" = "0",
        "dynamic_partition.reserved_history_periods" = "NULL",
        "dynamic_partition.storage_policy" = "",
        "dynamic_partition.storage_medium" = "HDD",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """ DROP TABLE IF EXISTS `entity_49_label_summary` """
    sql """
        CREATE TABLE `entity_49_label_summary` (
          `label_id` int(11) NOT NULL COMMENT '标签标识',
          `update_time` datetime NOT NULL COMMENT '更新时间',
          `label_value` varchar(128) NULL COMMENT '标签值',
          `label_type` int(11) NULL COMMENT '标签类型',
          `conf_score` DECIMAL(10, 2) NULL COMMENT '可信度分',
          `act_score` DECIMAL(10, 2) NULL COMMENT 'ID 活跃度',
          `source_dt` datetime NULL,
          `seq_id_bitmap` bitmap BITMAP_UNION NOT NULL COMMENT 'SeqID集合'
        ) ENGINE=OLAP
        AGGREGATE KEY(`label_id`, `update_time`, `label_value`, `label_type`, `conf_score`, `act_score`, `source_dt`)
        COMMENT '标签汇总表'
        PARTITION BY RANGE(`update_time`)
        (PARTITION p20220512 VALUES [('0000-01-01 00:00:00'), ('2022-05-13 00:00:00')),
        PARTITION p20220513 VALUES [('2022-05-13 00:00:00'), ('2022-05-14 00:00:00')),
        PARTITION p20241124 VALUES [('2024-11-24 00:00:00'), ('2024-11-25 00:00:00')))
        DISTRIBUTED BY HASH(`label_id`, `label_value`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.time_zone" = "Asia/Shanghai",
        "dynamic_partition.start" = "-2147483648",
        "dynamic_partition.end" = "2",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.buckets" = "3",
        "dynamic_partition.create_history_partition" = "false",
        "dynamic_partition.history_partition_num" = "-1",
        "dynamic_partition.hot_partition_num" = "0",
        "dynamic_partition.reserved_history_periods" = "NULL",
        "dynamic_partition.storage_policy" = "",
        "dynamic_partition.storage_medium" = "HDD",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    qt_sql_explode_bug_fix """
                with relation_bitmap as (
            select
                label_value,
                bitmap_union(seq_id_bitmap) as seq_id_bitmap
            from
                entity_37_label_summary
            where
                label_id = 1
                and label_type = 3
                and (
                    cast(source_dt as date) >= cast('2022-10-06' as date)
                )
                and (
                    cast(source_dt as date) <= cast('2022-10-06' as date)
                )
            group by
                label_value
        ),
        from_entity_bitmap as (
            (
                select
                    bitmap_intersect(a.seq_id_bitmap) as seq_id_bitmap
                from
                    (
                        select
                            bitmap_union(ifnull(b.seq_id_bitmap, bitmap_empty())) as seq_id_bitmap
                        from
                            (
                                select
                                    update_time
                                from
                                    (
                                        select
                                            1 tmp
                                    ) as a lateral view explode(['2024-11-21 13:38:33']) tmp as update_time
                            ) a
                            left join (
                                select
                                    *
                                from
                                    entity_49_label_summary
                                where
                                    label_id = 1606
                                    and label_type = 1
                                    and update_time in ('2024-11-21 13:38:33')
                                    and (
                                        cast(label_value as BOOLEAN) in (true, false)
                                    )
                            ) b on b.update_time = a.update_time
                        group by
                            a.update_time
                    ) a
            )
        )
        select
            IFNULL(bitmap_union(rb.seq_id_bitmap), bitmap_empty()) as seq_id_bitmap
        from
            (
                select
                    seq_id as seq_id
                from
                    from_entity_bitmap b lateral view explode_bitmap(b.seq_id_bitmap) tmp as seq_id
            ) __tt
            join relation_bitmap rb on rb.label_value = __tt.seq_id;
    """

    sql "SET enable_nereids_planner=true"
	qt_sql_explode_bitmap1 """ select dt, e1 from test_explode_bitmap lateral view explode_bitmap(user_id) tmp1 as e1 order by dt, e1;"""
}
