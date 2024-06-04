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
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.suite.SuiteCluster

suite("test_partition_create_tablet_rr") {
    def options = new ClusterOptions()
    options.beNum = 1
    options.feConfigs.add('disable_balance=true')
    def partition_disk_index_lru_size = 50
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'report_disk_state_interval_seconds=1',
        "partition_disk_index_lru_size=$partition_disk_index_lru_size"
    ]
    options.beDisks = ['HDD=4','SSD=4']
    options.enableDebugPoints()

    def checkTabletOnDiskTabletNumEq = {tbl ->
        sleep 5000

        def tablets = sql_return_maparray "SHOW TABLETS FROM $tbl"
        def pathTabletNum = [:]
        tablets.each {
            def num = pathTabletNum.get(it.PathHash)
            if (num) {
                pathTabletNum.put(it.PathHash, ++num)
            } else {
                pathTabletNum.put(it.PathHash, 1)
            }
        }

        log.info("table ${tbl} tablet in path ${pathTabletNum.values()}")
        def count = pathTabletNum.values().stream().distinct().count()
        assertEquals(count, 1)
    }

    docker(options) {
        sleep 2000
        def single_hdd_tbl = "single_HDD_tbl"
        def single_ssd_tbl = "single_SDD_tbl"
        def single_partition_tbl = "single_partition_tbl"
        sql """drop table if exists $single_hdd_tbl"""
        sql """drop table if exists $single_ssd_tbl"""
        sql """drop table if exists $single_partition_tbl"""
        for (def j = 0; j < partition_disk_index_lru_size + 10; j++) {
            def tbl = single_partition_tbl + j.toString()
            sql """drop table if exists $tbl"""
        }
        try {
            // 1. test single partition table
            //  a. create 1 table, has 100 buckets
            //  b. check disk's tablet num
        
            sql """
                CREATE TABLE $single_hdd_tbl (
                    `k1` int(11) NULL,
                    `k2` int(11) NULL
                ) DUPLICATE KEY(`k1`, `k2`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k1`) BUCKETS 12000
                PROPERTIES (
                    "replication_num"="1",
                    "storage_medium" = "HDD"
                );
            """

            checkTabletOnDiskTabletNumEq single_hdd_tbl

            sql """
                CREATE TABLE $single_ssd_tbl (
                    `k1` int(11) NULL,
                    `k2` int(11) NULL
                ) DUPLICATE KEY(`k1`, `k2`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k1`) BUCKETS 12000
                PROPERTIES (
                    "replication_num"="1",
                    "storage_medium" = "SSD"
                );
            """
            checkTabletOnDiskTabletNumEq single_ssd_tbl

            sql """
                CREATE TABLE $single_partition_tbl
                (
                    k1 DATE,
                    k2 DECIMAL(10, 2) DEFAULT "10.5",
                    k3 CHAR(10) COMMENT "string column",
                    k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
                )
                DUPLICATE KEY(k1, k2)
                PARTITION BY RANGE(k1)
                (
                    PARTITION p1 VALUES LESS THAN ("2020-02-01"),
                    PARTITION p2 VALUES LESS THAN ("2020-03-01"),
                    PARTITION p3 VALUES LESS THAN ("2020-04-01")
                )
                DISTRIBUTED BY HASH(k1) BUCKETS 320
                PROPERTIES (
                    "replication_num" = "1"
                );
            """
            checkTabletOnDiskTabletNumEq single_partition_tbl

            // 2. test multi thread create single partition tables
            //  a. multi thread create partition_disk_index_lru_size + 10 table
            //  b. check disk's tablet num
            def futures = []
            for (def i = 0; i < partition_disk_index_lru_size + 10; i++) {
                def tblMulti = single_partition_tbl + i.toString()
                futures.add(thread {
                            sql """
                                CREATE TABLE $tblMulti
                                (
                                    k1 DATE,
                                    k2 DECIMAL(10, 2) DEFAULT "10.5",
                                    k3 CHAR(10) COMMENT "string column",
                                    k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
                                )
                                DUPLICATE KEY(k1, k2)
                                PARTITION BY RANGE(k1)
                                (
                                    PARTITION p1 VALUES LESS THAN ("2020-02-01"),
                                    PARTITION p2 VALUES LESS THAN ("2020-03-01"),
                                    PARTITION p3 VALUES LESS THAN ("2020-04-01")
                                )
                                DISTRIBUTED BY HASH(k1) BUCKETS 320
                                PROPERTIES (
                                    "replication_num" = "1"
                                );
                            """
                            checkTabletOnDiskTabletNumEq tblMulti
                })
            }
            futures.each { it.get() }
        } finally {
                sql """drop table if exists $single_hdd_tbl"""
                sql """drop table if exists $single_ssd_tbl"""
                sql """drop table if exists $single_partition_tbl"""
                for (def j = 0; j < partition_disk_index_lru_size + 10; j++) {
                    def tbl = single_partition_tbl + j.toString()
                    sql """drop table if exists $tbl"""
                } 
        }
    }
}
