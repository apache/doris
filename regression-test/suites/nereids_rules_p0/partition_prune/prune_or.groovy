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

suite("prune_or") {
    multi_sql """
        drop table if exists prune_or_table;
        CREATE TABLE `prune_or_table` (
        `col_int_undef_signed_not_null` int NOT NULL,
        `col_date_undef_signed_not_null` date NOT NULL,
        `col_bigint_undef_signed_not_null` bigint NOT NULL,
        `col_int_undef_signed` int NULL,
        `col_bigint_undef_signed` bigint NULL,
        `col_date_undef_signed` date NULL,
        `col_varchar_10__undef_signed` varchar(10) NULL,
        `col_varchar_10__undef_signed_not_null` varchar(10) NOT NULL,
        `col_varchar_1024__undef_signed` varchar(1024) NULL,
        `col_varchar_1024__undef_signed_not_null` varchar(1024) NOT NULL,
        `pk` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`col_int_undef_signed_not_null`, `col_date_undef_signed_not_null`, `col_bigint_undef_signed_not_null`)
        PARTITION BY RANGE(`col_int_undef_signed_not_null`, `col_date_undef_signed_not_null`)
        (PARTITION p VALUES [("-2147483648", '0000-01-01'), ("-1", '1997-12-11')),
        PARTITION p0 VALUES [("-1", '1997-12-11'), ("4", '2023-12-11')),
        PARTITION p1 VALUES [("4", '2023-12-11'), ("6", '2023-12-15')),
        PARTITION p2 VALUES [("6", '2023-12-15'), ("7", '2023-12-16')),
        PARTITION p3 VALUES [("7", '2023-12-16'), ("8", '2023-12-25')),
        PARTITION p4 VALUES [("8", '2023-12-25'), ("8", '2024-01-18')),
        PARTITION p5 VALUES [("8", '2024-01-18'), ("10", '2024-02-18')),
        PARTITION p6 VALUES [("10", '2024-02-18'), ("1147483647", '2056-12-31')),
        PARTITION p100 VALUES [("1147483647", '2056-12-31'), ("2147483647", '9999-12-31')))
        DISTRIBUTED BY HASH(`col_bigint_undef_signed_not_null`) BUCKETS 30
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_mow_light_delete" = "false"
        );
        
        insert into prune_or_table values(6,'2025-02-18',-2148224693875975660,9,NULL,'2023-12-16','a','k','b','q',160);
        """


    order_qt_prune_or """
        select *
        from prune_or_table
        where (
            NOT (
                prune_or_table.`col_int_undef_signed_not_null` NOT BETWEEN 2.3 AND 5  -- true
                AND
                prune_or_table.`col_date_undef_signed_not_null` BETWEEN '2023-12-13' AND '2023-12-22' -- false
            )
        )
          AND prune_or_table.`col_date_undef_signed_not_null` >= '2024-01-19'
          AND prune_or_table.`col_int_undef_signed_not_null` > 4;
        """
}