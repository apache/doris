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

suite("map_agg") {
    sql "DROP TABLE IF EXISTS `test_map_agg`;"
    sql """
        CREATE TABLE IF NOT EXISTS `test_map_agg` (
            `id` int(11) NOT NULL,
            `label_name` varchar(32) NOT NULL,
            `value_field` string
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        insert into `test_map_agg` values
            (1, "LA", "V1_1"),
            (1, "LB", "V1_2"),
            (1, "LC", "V1_3"),
            (2, "LA", "V2_1"),
            (2, "LB", "V2_2"),
            (2, "LC", "V2_3"),
            (3, "LA", "V3_1"),
            (3, "LB", "V3_2"),
            (3, "LC", "V3_3"),
            (4, "LA", "V4_1"),
            (4, "LB", "V4_2"),
            (4, "LC", "V4_3"),
            (5, "LA", "V5_1"),
            (5, "LB", "V5_2"),
            (5, "LC", "V5_3");
    """

    sql "DROP TABLE IF EXISTS test_map_agg_nullable;"
    sql """
        CREATE TABLE IF NOT EXISTS `test_map_agg_nullable` (
             `id` int(11) NOT NULL,
             `label_name` varchar(32) NULL,
             `value_field` string
          ) ENGINE=OLAP
          DUPLICATE KEY(`id`)
          COMMENT 'OLAP'
          DISTRIBUTED BY HASH(`id`) BUCKETS 10
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "storage_format" = "V2",
          "light_schema_change" = "true",
          "disable_auto_compaction" = "false",
          "enable_single_replica_compaction" = "false"
          );
     """
    sql """
        insert into `test_map_agg_nullable` values
            (1, "LA", "V1_1"),
            (1, "LB", "V1_2"),
            (1, "LC", null),
            (2, "LA", "V2_1"),
            (2,  null, "V2_2"),
            (2, "LC", "V2_3"),
            (3, "LA", "V3_1"),
            (3, "LB", "V3_2"),
            (3, "LC", "V3_3"),
            (4, "LA", "V4_1"),
            (4, "LB", "V4_2"),
            (4, null, null),
            (5, "LA", "V5_1"),
            (5, "LB", "V5_2"),
            (5, "LC", "V5_3");
     """

    sql "DROP TABLE IF EXISTS `test_map_agg_numeric_key`;"
    sql """
        CREATE TABLE IF NOT EXISTS `test_map_agg_numeric_key` (
            `id` int(11) NOT NULL,
            `label_name` bigint NOT NULL,
            `value_field` string
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
      """

    sql """
         insert into `test_map_agg_numeric_key` values
             (1, 1, "V1_1"),
             (1, 9223372036854775807, "V1_2"),
             (1, 22000000000, "V1_3"),
             (2, 1, "V2_1"),
             (2, 9223372036854775807, "V2_2"),
             (2, 22000000000, "V2_3"),
             (3, 1, "V3_1"),
             (3, 9223372036854775807, "V3_2"),
             (3, 22000000000, "V3_3"),
             (4, 1, "V4_1"),
             (4, 9223372036854775807, "V4_2"),
             (4, 22000000000, "V4_3"),
             (5, 1, "V5_1"),
             (5, 9223372036854775807, "V5_2"),
             (5, 22000000000, "V5_3");
    """

    sql "DROP TABLE IF EXISTS `test_map_agg_decimal`;"
    sql """
         CREATE TABLE IF NOT EXISTS `test_map_agg_decimal` (
             `id` int(11) NOT NULL,
             `label_name` string NOT NULL,
             `value_field` decimal(15,4)
         ) ENGINE=OLAP
         DUPLICATE KEY(`id`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`id`) BUCKETS 2
         PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "storage_format" = "V2",
         "light_schema_change" = "true",
         "disable_auto_compaction" = "false",
         "enable_single_replica_compaction" = "false"
         );
    """

    sql """
        insert into `test_map_agg_decimal` values
          (1, "k1", 1.2345),
          (1, "k2", 2.4567),
          (1, "k3", 5.9876),
          (2, "k1", 2.4567),
          (2, "k2", 3.33),
          (2, "k3", 4.55),
          (3, "k1", 188.998),
          (3, "k2", 998.996),
          (3, "k3", 1024.1024)
    """

    sql "DROP TABLE IF EXISTS `test_map_agg_score`;"
    sql """
        CREATE TABLE `test_map_agg_score`(
            id INT(11) NOT NULL,
            userid VARCHAR(20) NOT NULL COMMENT '用户id',
            subject VARCHAR(20) COMMENT '科目',
            score DOUBLE COMMENT '成绩'
        )
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        INSERT INTO `test_map_agg_score`  VALUES (1,'001','语文',90);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (2,'001','数学',92);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (3,'001','英语',80);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (4,'002','语文',88);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (5,'002','数学',90);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (6,'002','英语',75.5);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (7,'003','语文',70);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (8,'003','数学',85);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (9,'003','英语',90);
    """
    sql """
        INSERT INTO `test_map_agg_score`  VALUES (10,'003','政治',82);
    """

    qt_sql1 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg GROUP BY `id`
        )
        SELECT
            id,
            m['LA'] LA,
            m['LB'] LB,
            m['LC'] LC
        FROM `labels`
        ORDER BY `id`;
     """

    qt_sql2 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_nullable GROUP BY `id`
        )
        SELECT
            id,
            m['LA'] LA,
            m['LB'] LB,
            m['LC'] LC
        FROM `labels`
        ORDER BY `id`;
     """

    qt_sql3 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_numeric_key GROUP BY `id`
        )
        SELECT
            id,
            m[1] LA,
            m[9223372036854775807] LB,
            m[22000000000] LC
        FROM `labels`
        ORDER BY `id`;
    """

    qt_sql4 """
        select map_agg(k, v) from (select 'key' as k, array('ab', 'efg', null) v) a;
    """

    qt_sql5 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_decimal GROUP BY `id`
        )
        SELECT
            id,
            m["k1"] LA,
            m["k2"] LB,
            m["k3"] LC
        FROM `labels`
        ORDER BY `id`;
    """

    qt_sql6 """
        select m['LC'] from (SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_nullable GROUP BY `id`)t order by 1;
    """

    qt_garbled_characters """
        select
            userid, map['语文'] 语文, map['数学'] 数学, map['英语'] 英语, map['政治'] 政治
        from (
            select userid, map_agg(subject,score) as map from test_map_agg_score group by userid
        ) a order by userid;
    """

    sql "DROP TABLE IF EXISTS test_map_agg_multi;"
    sql """
        create table test_map_agg_multi (
            data_time bigint,
            mil int,
            vin string,
            car_type string,
            month string,
            day string
        ) engine=olap
        distributed by hash(data_time) buckets 10
        properties("replication_num" = "1");
    """
    sql """
        insert into test_map_agg_multi values (1, 1, 'abc', 'bc', '01', '01'), (2, 2, 'abc', 'bc', '01', '01');
    """

    qt_multi """
        select
            m1['1']
            , m2['2']
        from (
            select
                vin
                , car_type
                , map_agg(ts, mile) m1
                , map_agg(mile, ts) m2
            from (
                 select
                    vin
                    , car_type
                    , data_time as ts
                    , mil as mile, month
                    , day from test_map_agg_multi
            )a
            group by
               car_type
               , vin
               , month
               , day
        ) t order by 1, 2;
    """
 }
