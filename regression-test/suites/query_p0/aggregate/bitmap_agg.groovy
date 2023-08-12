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

suite("bitmap_agg") {
    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"
    sql """
        CREATE TABLE `test_bitmap_agg` (
            `id` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        insert into `test_bitmap_agg`
        select number from numbers("number" = "20000");
    """

    qt_sql1 """
        select bitmap_count(bitmap_agg(id)) from `test_bitmap_agg`;
    """

    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"

    sql "DROP TABLE IF EXISTS test_bitmap_agg_nullable;"
    sql """
        CREATE TABLE `test_bitmap_agg_nullable` (
             `id` int(11) NULL
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
        insert into `test_bitmap_agg_nullable`
                select number from numbers("number" = "20000");
    """
    qt_sql2 """
        select bitmap_count(bitmap_agg(id)) from `test_bitmap_agg_nullable`;
    """
    sql "DROP TABLE IF EXISTS `test_bitmap_agg`;"

 }
