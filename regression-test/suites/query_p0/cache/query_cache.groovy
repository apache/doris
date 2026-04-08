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

import java.util.stream.Collectors

suite("query_cache") {
    def tableName = "table_3_undef_partitions2_keys3_properties4_distributed_by53"

    sql "set enable_sql_cache=false"

    def test = {
        sql "set enable_query_cache=false"

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE ${tableName} (
            `pk` int NULL,
            `col_varchar_10__undef_signed` varchar(10) NULL,
            `col_int_undef_signed` int NULL,
            `col_varchar_1024__undef_signed` varchar(1024) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`pk`, `col_varchar_10__undef_signed`)
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V3",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
        )
    """

        sql """
        INSERT INTO ${tableName}(pk, col_varchar_10__undef_signed, col_int_undef_signed, col_varchar_1024__undef_signed) 
        VALUES
            (0, "mean", null, "p"),
            (1, "is", 6, "what"),
            (2, "one", null, "e")
    """

        // First complex query - Run without cache
        order_qt_query_cache1 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        // Simple query - Run without cache
        order_qt_query_cache2 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        // Enable query cache
        sql "set enable_query_cache=true"

        // Run the same complex query with cache enabled
        order_qt_query_cache3 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        // Run the same simple query with cache enabled
        order_qt_query_cache4 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        // Run both queries again to test cache hit
        order_qt_query_cache5 """
        SELECT 
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName} AS alias1
        WHERE (
            alias1.col_varchar_1024__undef_signed LIKE CONCAT('aEIovabVCD', '%')
            AND (
                (alias1.`pk` = 154 OR (
                    alias1.col_varchar_1024__undef_signed LIKE CONCAT('lWpWJPFqXM', '%')
                    AND alias1.`pk` = 111
                ))
                AND (
                    alias1.col_varchar_10__undef_signed != 'IfGTFZuqZr'
                    AND alias1.col_varchar_1024__undef_signed > 'with'
                )
                AND alias1.`pk` IS NULL
            )
            AND alias1.col_int_undef_signed < 7
        )
        GROUP BY field3
    """

        order_qt_query_cache6 """
        SELECT
            MIN(`pk`) AS field1,
            MAX(`pk`) AS field2,
            `pk` AS field3
        FROM ${tableName}
        GROUP BY field3
    """

        order_qt_query_cache7 """
        SELECT
        col_int_undef_signed,
            MIN(`col_int_undef_signed`) AS field1,
            MAX(`col_int_undef_signed`) AS field2,
            COUNT(`col_int_undef_signed`) AS field3,
            SUM(`col_int_undef_signed`) AS field4
        FROM ${tableName}
        GROUP BY col_int_undef_signed
    """

        // reorder the order_qt_query_cache7 select list to test the cache hit
        order_qt_query_cache8 """
 SELECT
    COUNT(`col_int_undef_signed`) AS field3,  -- Count of col_int_undef_signed (Original field3)
    col_int_undef_signed,                      -- The original unsigned integer column (Original col_int_undef_signed)
    SUM(`col_int_undef_signed`) AS field4,     -- Sum of col_int_undef_signed (Original field4)
    MIN(`col_int_undef_signed`) AS field1,     -- Minimum value of col_int_undef_signed (Original field1)
    MAX(`col_int_undef_signed`) AS field2      -- Maximum value of col_int_undef_signed (Original field2). Note: Trailing comma removed to avoid syntax error.
FROM ${tableName}
GROUP BY col_int_undef_signed;
    """
    }

    sql "set enable_nereids_distribute_planner=false"
    test()

    sql "set enable_nereids_distribute_planner=true"
    test()


    multi_sql """
        drop table if exists query_cache_list_table;
        CREATE TABLE `query_cache_list_table` (
          `id` bigint NOT NULL AUTO_INCREMENT(1),
          `commit_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
          `data_id` bigint NOT NULL DEFAULT "0" ,
          `entity_type` varchar(64) NOT NULL DEFAULT "" ,
          `data_trace_id` bigint NOT NULL DEFAULT "0" ,
          `action_id` bigint NOT NULL DEFAULT "0" ,
          `city_code` int NOT NULL DEFAULT "0" ,
          `source_id` varchar(32) NOT NULL DEFAULT "" ,
          `property` varchar(128) NOT NULL DEFAULT "" ,
          `commit_value` varchar(64000) NOT NULL DEFAULT "" ,
          `quality_score` float NOT NULL DEFAULT "0" ,
          `ext_info` varchar(1024) NOT NULL DEFAULT "" ,
          `state` tinyint NOT NULL DEFAULT "1" ,
          `ctime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,
          `mtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,
          `cuser` bigint NOT NULL DEFAULT "0" ,
          `muser` bigint NOT NULL DEFAULT "0" 
        ) ENGINE=OLAP
        UNIQUE KEY(`id`, `commit_time`, `data_id`, `entity_type`)
        PARTITION BY LIST (`entity_type`)
        (PARTITION p_aoi VALUES IN ("AOI"),
        PARTITION p_loi VALUES IN ("LOI"),
        PARTITION p_poi VALUES IN ("POI"),
        PARTITION p_bizcircle VALUES IN ("bizcircle"),
        PARTITION p_district VALUES IN ("district"),
        PARTITION p_house VALUES IN ("house"),
        PARTITION p_nh_block VALUES IN ("nh_block"),
        PARTITION p_nh_building VALUES IN ("nh_building"),
        PARTITION p_nh_frame VALUES IN ("nh_frame"),
        PARTITION p_nh_land VALUES IN ("nh_land"),
        PARTITION p_nh_project VALUES IN ("nh_project"),
        PARTITION p_sell_bizcircle VALUES IN ("sell_bizcircle"),
        PARTITION p_sell_building VALUES IN ("sell_building"),
        PARTITION p_sell_floor VALUES IN ("sell_floor"),
        PARTITION p_sell_house VALUES IN ("sell_house"),
        PARTITION p_sell_resblock VALUES IN ("sell_resblock"),
        PARTITION p_sell_unit VALUES IN ("sell_unit"))
        DISTRIBUTED BY HASH(`data_id`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "true",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "binlog.enable" = "true",
        "binlog.ttl_seconds" = "86400",
        "binlog.max_bytes" = "9223372036854775807",
        "binlog.max_history_nums" = "9223372036854775807",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_mow_light_delete" = "false"
        ); 
        set disable_nereids_rules=PRUNE_EMPTY_PARTITION;
        """

    sql "SELECT count(1) FROM query_cache_list_table"

    for (int i = 0; i < 3; ++i) {
        multi_sql """
            set enable_query_cache=false;
            drop table if exists query_cache_schema_change1;
            create table query_cache_schema_change1(
              id int,
              value int
            )
            partition by range(id)(
              partition p1 values[('1'), ('2')),
              partition p2 values[('2'), ('3')),
              partition p3 values[('3'), ('4')),
              partition p4 values[('4'), ('5')),
              partition p5 values[('5'), ('6'))
            )distributed by hash(id)
            properties('replication_num'='1');
            
            insert into query_cache_schema_change1 values (1, 1), (1, 2),(2, 1), (2, 2), (3, 1), (3, 2),(4, 1), (4, 2),(5, 1), (5, 2);
            
            set enable_query_cache=true;
        """

        explain {
            sql ("select id, count(value) from query_cache_schema_change1 group by id order by id")
            contains("QUERY_CACHE")
        }
    }
} 
