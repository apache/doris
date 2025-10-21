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

suite("test_auto_partition_with_single_replica_insert") {
    def tableName1 = "test_auto_partition_with_single_replica_insert_1"
    def tableName2 = "test_auto_partition_with_single_replica_insert_2"
    sql "drop table if exists ${tableName1}"
    sql """
        CREATE TABLE `${tableName1}` (
            `user_id` varchar(100) NULL,
            `goods_id` varchar(100) NULL,
            `dates` date NULL,
            `chain_name` varchar(100) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `goods_id`, `dates`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`chain_name`)
        (PARTITION pchain5fname10 VALUES IN ("chain_name"),
        PARTITION p4e0995e85ce1534e4e3a5 VALUES IN ("星辰医疗科技有限公司"))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    streamLoad {
        table "${tableName1}"
        set 'column_separator', ','
        file "test_auto_partition_with_single_replica_insert.csv"
        time 20000
    }
    sql " sync "
    qt_select1 "select * from ${tableName1} order by user_id"
    def result1 = sql "show partitions from ${tableName1}"
    logger.info("${result1}")
    assertEquals(result1.size(), 79)

    sql "drop table if exists ${tableName2}"
    sql """
        CREATE TABLE `${tableName2}` (
            `user_id` varchar(100) NULL,
            `goods_id` varchar(100) NULL,
            `dates` date NULL,
            `chain_name` varchar(100) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `goods_id`, `dates`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`chain_name`)
        (PARTITION pchain5fname10 VALUES IN ("chain_name"),
        PARTITION p4e0995e85ce1534e4e3a5 VALUES IN ("星辰医疗科技有限公司"))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """set experimental_enable_nereids_planner = true"""
    sql """set enable_memtable_on_sink_node = false"""
    sql """set experimental_enable_single_replica_insert = true"""
    sql "insert into ${tableName2} select user_id, goods_id, dates, chain_name from ${tableName1}"
    sql " sync "
    qt_select2 "select * from ${tableName2} order by user_id"
    def result2 = sql "show partitions from ${tableName1}"
    logger.info("${result2}")
    assertEquals(result1.size(), 79)
}
