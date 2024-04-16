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

suite("test_join_with_cast_array", "query,p0") {
    sql "drop table if exists dwd_channel_account_consume_history_reconciliation"
    sql """ CREATE TABLE `dwd_channel_account_consume_history_reconciliation` (
      `date` date NOT NULL,
      `media_id` int(11) NOT NULL,
      `plat_username` varchar(360) NOT NULL,
      `customer_id` varchar(360) NULL,
      `business_line` tinyint(4) NULL DEFAULT "3",
      `group_id` bigint(20) NULL,
      `group_name` varchar(192) NULL,
      `customer_name` varchar(765) NULL,
    ) ENGINE=OLAP
    UNIQUE KEY(`date`, `media_id`, `plat_username`)
    COMMENT 'OLAP'
    PARTITION BY RANGE(`date`)
    (PARTITION p201912 VALUES [('0000-01-01'), ('2020-01-01')),
    PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01')),
    PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01')),
    PARTITION p202003 VALUES [('2020-03-01'), ('2020-04-01')),
    PARTITION p202004 VALUES [('2020-04-01'), ('2020-05-01')),
    PARTITION p202005 VALUES [('2020-05-01'), ('2020-06-01')),
    PARTITION p202006 VALUES [('2020-06-01'), ('2020-07-01')),
    PARTITION p202007 VALUES [('2020-07-01'), ('2020-08-01')),
    PARTITION p202008 VALUES [('2020-08-01'), ('2020-09-01')),
    PARTITION p202009 VALUES [('2020-09-01'), ('2020-10-01')),
    PARTITION p202010 VALUES [('2020-10-01'), ('2020-11-01')),
    PARTITION p202011 VALUES [('2020-11-01'), ('2020-12-01')),
    PARTITION p202012 VALUES [('2020-12-01'), ('2021-01-01')),
    PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01')),
    PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01')),
    PARTITION p202103 VALUES [('2021-03-01'), ('2021-04-01')),
    PARTITION p202104 VALUES [('2021-04-01'), ('2021-05-01')),
    PARTITION p202407 VALUES [('2024-07-01'), ('2024-08-01')))
    DISTRIBUTED BY HASH(`plat_username`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.time_zone" = "Asia/Shanghai",
    "dynamic_partition.start" = "-2147483648",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.replication_allocation" = "tag.location.default: 1",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "false",
    "dynamic_partition.history_partition_num" = "-1",
    "dynamic_partition.hot_partition_num" = "0",
    "dynamic_partition.reserved_history_periods" = "NULL",
    "dynamic_partition.storage_policy" = "",
    "dynamic_partition.storage_medium" = "HDD",
    "dynamic_partition.start_day_of_month" = "1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "binlog.enable" = "false",
    "binlog.ttl_seconds" = "86400",
    "binlog.max_bytes" = "9223372036854775807",
    "binlog.max_history_nums" = "9223372036854775807",
    "enable_single_replica_compaction" = "false"
    );   """

    sql "drop table if exists dwd_oa_customer"
    sql """ CREATE TABLE `dwd_oa_customer` (
      `customer_id` bigint(20) NOT NULL,
      `customer_name` varchar(765) NOT NULL,
      `business_type` tinyint(4) NULL,
      `business_line` varchar(192) NULL DEFAULT "3",
      `subject_name` varchar(192) NULL,
    ) ENGINE=OLAP
    UNIQUE KEY(`customer_id`, `customer_name`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`customer_id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "binlog.enable" = "false",
    "binlog.ttl_seconds" = "86400",
    "binlog.max_bytes" = "9223372036854775807",
    "binlog.max_history_nums" = "9223372036854775807",
    "enable_single_replica_compaction" = "false"
    );"""

    sql "drop table if exists tb_media"
    sql """ CREATE TABLE `tb_media` (
      `id` int(11) NOT NULL,
      `name` varchar(360) NULL,
      `media_type` tinyint(4) NULL,
      `business_line` tinyint(4) NULL DEFAULT "3",
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "binlog.enable" = "false",
    "binlog.ttl_seconds" = "86400",
    "binlog.max_bytes" = "9223372036854775807",
    "binlog.max_history_nums" = "9223372036854775807",
    "enable_single_replica_compaction" = "false"
    ); """

    sql "drop table if exists tb_reconciliation_detail"
    sql """ CREATE TABLE `tb_reconciliation_detail` (
      `id` int(11) NOT NULL,
      `ysk_serial_number` varchar(96) NOT NULL DEFAULT "",
      `settlement_time` int(11) NOT NULL DEFAULT "0",
    ) ENGINE=OLAP
    UNIQUE KEY(`id`, `ysk_serial_number`, `settlement_time`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`, `ysk_serial_number`, `settlement_time`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "bloom_filter_columns" = "ysk_serial_number, id, settlement_time",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "compression" = "LZ4",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "binlog.enable" = "false",
    "binlog.ttl_seconds" = "86400",
    "binlog.max_bytes" = "9223372036854775807",
    "binlog.max_history_nums" = "9223372036854775807",
    "enable_single_replica_compaction" = "false"
    ); """

    sql "drop table if exists tb_reconciliation_media_order"
    sql """ CREATE TABLE `tb_reconciliation_media_order` (
      `id` int(11) NOT NULL,
      `customer_id` int(11) NOT NULL DEFAULT "0",
      `media_id` int(11) NOT NULL DEFAULT "0",
      `project_id` int(11) NOT NULL DEFAULT "0",
      `business_line` tinyint(4) NOT NULL DEFAULT "0",
      `business_type` tinyint(4) NOT NULL DEFAULT "0",
      `ysk_serial_number` varchar(96) NOT NULL DEFAULT "",
      `settlement_time` int(11) NOT NULL DEFAULT "0",
      `type` tinyint(4) NOT NULL DEFAULT "0",
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "compression" = "LZ4",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "binlog.enable" = "false",
    "binlog.ttl_seconds" = "86400",
    "binlog.max_bytes" = "9223372036854775807",
    "binlog.max_history_nums" = "9223372036854775807",
    "enable_single_replica_compaction" = "false"
    );"""

    def stream_load_by_name = {tableName, tblRows ->
        streamLoad {
            set 'column_separator', '\t'
            table "${tableName}"
            time 10000
            file "${tableName}"+'.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(tblRows, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    stream_load_by_name("dwd_channel_account_consume_history_reconciliation", 200)
    stream_load_by_name("dwd_oa_customer", 200)
    stream_load_by_name("tb_media", 10)
    stream_load_by_name("tb_reconciliation_detail", 200)
    stream_load_by_name("tb_reconciliation_media_order", 200)

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false;"""
    order_qt_sql """
    SELECT temp_l.media_id
    FROM   (SELECT `business_line`,
                   Concat_ws(',', Collect_set(media_id))                     AS
                   media_id,
                   Concat_ws(',', Collect_set(group_name))                   AS
                   group_name,
                   Concat_ws(',', Collect_set(Date_format(`date`, '%Y-%m'))) AS date
            FROM   dwd_channel_account_consume_history_reconciliation
            WHERE  `date` <> Curdate()
                   AND business_line IN ( 3 )
            GROUP  BY business_line,
                      media_id,
                      Date_format(`date`, '%Y-%m')) AS temp_l
           full OUTER JOIN (SELECT t.business_line,
                                   Concat_ws(',', Collect_set(t.media_id))        AS
           media_id,
                                   Concat_ws(',', Collect_set(t.group_name))      AS
           group_name,
                                   Concat_ws(',', Collect_set(t.settlement_time)) AS
           settlement_time
                            FROM   ((SELECT rmo.business_line,
                                            m.id
                                            AS
                                            media_id,
                                            dwd_oa_customer.subject_name
                                            AS
                                            group_name,
                                            From_unixtime(rd.settlement_time,
                                            '%Y-%m') AS
                                            `settlement_time`
                                     FROM   tb_reconciliation_detail rd
                                            LEFT JOIN tb_reconciliation_media_order
                                                      rmo
                                                   ON rmo.ysk_serial_number =
                                                      rd.ysk_serial_number
                                            LEFT JOIN tb_media m
                                                   ON m.id = rmo.media_id
                                            LEFT JOIN dwd_oa_customer
                                                   ON dwd_oa_customer.customer_id =
                                                      rmo.customer_id
                                                      AND
                                            dwd_oa_customer.business_line =
                                            rmo.business_line
                                     WHERE  rmo.type <> 3
                                            AND rmo.business_line IN ( '3' ))
                                    UNION
                                    (SELECT rmo.business_line,
                                            m.id
                                            AS
                                            media_id,
                                            dwd_oa_customer.subject_name
                                            AS
                                            group_name,
                                            From_unixtime(rmo.settlement_time,
                                            '%Y-%m') AS
                                            `settlement_time`
                                     FROM   tb_reconciliation_media_order rmo
                                            LEFT JOIN tb_media m
                                                   ON m.id = rmo.media_id
                                            LEFT JOIN dwd_oa_customer
                                                   ON dwd_oa_customer.customer_id =
                                                      rmo.customer_id
                                                      AND
                                            dwd_oa_customer.business_line =
                                            rmo.business_line
                                     WHERE  rmo.type <> 3
                                            AND rmo.business_line IN ( '3' )
                                            AND NOT
                                    EXISTS (SELECT 1
                                            FROM   tb_reconciliation_detail
                                                   rd
                                            WHERE  rd.ysk_serial_number =
                                                   rmo.ysk_serial_number
                                                   AND rd.settlement_time =
           rmo.settlement_time)))
           t
           GROUP  BY business_line,
           media_id,
           settlement_time) AS temp_r
                        ON temp_l.business_line = temp_r.business_line
                           AND temp_l.media_id = temp_r.media_id
                           AND temp_l.group_name = temp_r.group_name;
    """
}
