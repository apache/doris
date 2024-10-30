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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/auto-increment.md") {
    try {
        multi_sql "create database if not exists demo; use demo;"
        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `value` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `id` BIGINT NOT NULL AUTO_INCREMENT(100),
              `value` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `uid` BIGINT NOT NULL,
              `name` BIGINT NOT NULL,
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `value` BIGINT NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`uid`, `name`)
        DISTRIBUTED BY HASH(`uid`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `name` varchar(65533) NOT NULL,
              `value` int(11) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `text` varchar(65533) NOT NULL,
              `id` BIGINT NOT NULL AUTO_INCREMENT,
        ) ENGINE=OLAP
        UNIQUE KEY(`text`)
        DISTRIBUTED BY HASH(`text`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
              `text` varchar(65533) NOT NULL,
              `id` BIGINT NOT NULL AUTO_INCREMENT,
        ) ENGINE=OLAP
        UNIQUE KEY(`text`)
        DISTRIBUTED BY HASH(`text`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `demo`.`tbl`"
        sql """
        CREATE TABLE `demo`.`tbl` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `name` varchar(65533) NOT NULL,
            `value` int(11) NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """
        multi_sql """
        insert into tbl(name, value) values("Bob", 10), ("Alice", 20), ("Jack", 30);
        select * from tbl order by id;
        """

        cmd """
        curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "columns:name,value" -H "column_separator:," -T ${context.file.parent}/test_data/test.csv http://${context.config.feHttpAddress}/api/demo/tbl/_stream_load
        """
        sql "select * from tbl order by id"

        multi_sql """
        insert into tbl(id, name, value) values(null, "Doris", 60), (null, "Nereids", 70);
        select * from tbl order by id;
        """

        sql "drop table if exists `demo`.`tbl2`"
        multi_sql """
        CREATE TABLE `demo`.`tbl2` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `name` varchar(65533) NOT NULL,
            `value` int(11) NOT NULL DEFAULT "0"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        );

        insert into tbl2(id, name, value) values(1, "Bob", 10), (2, "Alice", 20), (3, "Jack", 30);
        select * from tbl2 order by id;
        
        set enable_unique_key_partial_update=true;
        set enable_insert_strict=false;
        insert into tbl2(id, name) values(1, "modified"), (4, "added");

        select * from tbl2 order by id;
        """

        sql "drop table if exists `demo`.`tbl3`"
        multi_sql """
        CREATE TABLE `demo`.`tbl3` (
            `id` BIGINT NOT NULL,
            `name` varchar(100) NOT NULL,
            `score` BIGINT NOT NULL,
            `aid` BIGINT NOT NULL AUTO_INCREMENT
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        );

        insert into tbl3(id, name, score) values(1, "Doris", 100), (2, "Nereids", 200), (3, "Bob", 300);
        select * from tbl3 order by id;
        
        set enable_unique_key_partial_update=true;
        set enable_insert_strict=false;
        insert into tbl3(id, score) values(1, 999), (2, 888);
        select * from tbl3 order by id;

        insert into tbl3(id, aid) values(1, 1000), (3, 500);
        select * from tbl3 order by id;
        """

        sql "drop table if exists `demo`.`dwd_dup_tbl`"
        sql """
        CREATE TABLE `demo`.`dwd_dup_tbl` (
            `user_id` varchar(50) NOT NULL,
            `dim1` varchar(50) NOT NULL,
            `dim2` varchar(50) NOT NULL,
            `dim3` varchar(50) NOT NULL,
            `dim4` varchar(50) NOT NULL,
            `dim5` varchar(50) NOT NULL,
            `visit_time` DATE NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """
        sql "drop table if exists `demo`.`dictionary_tbl`"
        sql """
        CREATE TABLE `demo`.`dictionary_tbl` (
            `user_id` varchar(50) NOT NULL,
            `aid` BIGINT NOT NULL AUTO_INCREMENT
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        )
        """
        sql """
        insert into dictionary_tbl(user_id)
        select user_id from dwd_dup_tbl group by user_id
        """
        sql """
        insert into dictionary_tbl(user_id)
        select dwd_dup_tbl.user_id from dwd_dup_tbl left join dictionary_tbl
        on dwd_dup_tbl.user_id = dictionary_tbl.user_id where dwd_dup_tbl.visit_time > '2023-12-10' and dictionary_tbl.user_id is NULL
        """
        sql "drop table if exists `demo`.`dws_agg_tbl`"
        sql """
        CREATE TABLE `demo`.`dws_agg_tbl` (
            `dim1` varchar(50) NOT NULL,
            `dim3` varchar(50) NOT NULL,
            `dim5` varchar(50) NOT NULL,
            `user_id_bitmap` BITMAP BITMAP_UNION NOT NULL,
            `pv` BIGINT SUM NOT NULL 
        ) ENGINE=OLAP
        AGGREGATE KEY(`dim1`,`dim3`,`dim5`)
        DISTRIBUTED BY HASH(`dim1`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """
        sql """
        insert into dws_agg_tbl
        select dwd_dup_tbl.dim1, dwd_dup_tbl.dim3, dwd_dup_tbl.dim5, BITMAP_UNION(TO_BITMAP(dictionary_tbl.aid)), COUNT(1)
        from dwd_dup_tbl INNER JOIN dictionary_tbl on dwd_dup_tbl.user_id = dictionary_tbl.user_id
        group by dwd_dup_tbl.dim1, dwd_dup_tbl.dim3, dwd_dup_tbl.dim5
        """
        sql """
        select dim1, dim3, dim5, bitmap_count(user_id_bitmap) as uv, pv from dws_agg_tbl
        """

        sql "drop table if exists `demo`.`records_tbl`"
        sql """
        CREATE TABLE `demo`.`records_tbl` (
            `user_id` int(11) NOT NULL COMMENT "",
            `name` varchar(26) NOT NULL COMMENT "",
            `address` varchar(41) NOT NULL COMMENT "",
            `city` varchar(11) NOT NULL COMMENT "",
            `nation` varchar(16) NOT NULL COMMENT "",
            `region` varchar(13) NOT NULL COMMENT "",
            `phone` varchar(16) NOT NULL COMMENT "",
            `mktsegment` varchar(11) NOT NULL COMMENT ""
        ) DUPLICATE KEY (`user_id`, `name`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """
        sql "select * from records_tbl order by user_id, name limit 100"
        sql "select * from records_tbl order by user_id, name limit 100 offset 100"

        sql "drop table if exists `demo`.`records_tbl2`"
        sql """
        CREATE TABLE `demo`.`records_tbl2` (
            `user_id` int(11) NOT NULL COMMENT "",
            `name` varchar(26) NOT NULL COMMENT "",
            `address` varchar(41) NOT NULL COMMENT "",
            `city` varchar(11) NOT NULL COMMENT "",
            `nation` varchar(16) NOT NULL COMMENT "",
            `region` varchar(13) NOT NULL COMMENT "",
            `phone` varchar(16) NOT NULL COMMENT "",
            `mktsegment` varchar(11) NOT NULL COMMENT "",
            `unique_value` BIGINT NOT NULL AUTO_INCREMENT
        ) DUPLICATE KEY (`user_id`, `name`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """
        sql "select * from records_tbl2 order by unique_value limit 100"
        sql "select * from records_tbl2 where unique_value > 99 order by unique_value limit 100"
        sql """
        select user_id, name, address, city, nation, region, phone, mktsegment
        from records_tbl2, (select unique_value as max_value from records_tbl2 order by unique_value limit 1 offset 9999) as previous_data
        where records_tbl2.unique_value > previous_data.max_value
        order by unique_value limit 100
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/auto-increment.md failed to exec, please fix it", t)
    }
}
