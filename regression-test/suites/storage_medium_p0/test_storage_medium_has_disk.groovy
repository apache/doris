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

suite('test_storage_medium_has_disk') {
    if (isCloudMode()) {
        return
    }

    def checkPartitionMedium = { table, isHdd ->
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${table}"
        assertTrue(partitions.size() > 0)
        partitions.each { assertEquals(isHdd ? 'HDD' : 'SSD', it.StorageMedium) }
    }

    def checkTableStaticPartition = { isPartitionTable, isHdd ->
        def table = "tbl_static_${isPartitionTable}_${isHdd}"
        def sqlText = "CREATE TABLE ${table} (k INT)"
        if (isPartitionTable) {
            sqlText += " PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))"
        }
        sqlText += ' DISTRIBUTED BY HASH(k) BUCKETS 5'
        if (isHdd) {
            sqlText += " PROPERTIES ('storage_medium' = 'hdd')"
        }
        sql sqlText
        checkPartitionMedium table, isHdd
    }

    def beReportTablet = { ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        backendId_to_backendIP.each { beId, beIp ->
            def port = backendId_to_backendHttpPort.get(beId) as int
            be_report_tablet beIp, port
        }
        sleep 8000
    }

    def options = new ClusterOptions()
    options.feConfigs += ['default_storage_medium=SSD', 'dynamic_partition_check_interval_seconds=1']
    options.beConfigs += ['report_disk_state_interval_seconds=1']
    options.beDisks = ['HDD=1', 'SSD=1']
    docker(options) {
        // test static partitions
        for (def isPartitionTable : [true, false]) {
            for (def isHdd : [true, false]) {
                checkTableStaticPartition isPartitionTable, isHdd
            }
        }

        //test dynamic partition without hot partitions
        def table = 'tbl_dynamic_1'
        sql """CREATE TABLE ${table} (k DATE)
        PARTITION BY RANGE(k)()
        DISTRIBUTED BY HASH (k) BUCKETS 5
        PROPERTIES
        (
            "storage_medium" = "hdd",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.end" = "1",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "3",
            "dynamic_partition.replication_num" = "1",
            "dynamic_partition.create_history_partition"= "true",
            "dynamic_partition.start" = "-3"
        )
        """
        checkPartitionMedium table, false

        table = 'tbl_dynamic_2'
        sql """CREATE TABLE ${table} (k DATE)
        PARTITION BY RANGE(k)()
        DISTRIBUTED BY HASH (k) BUCKETS 5
        PROPERTIES
        (
            "storage_medium" = "ssd",
            "dynamic_partition.storage_medium" = "hdd",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "3",
            "dynamic_partition.replication_num" = "1",
            "dynamic_partition.create_history_partition"= "true",
            "dynamic_partition.start" = "-4"
        )
        """
        checkPartitionMedium table, true

        // test dynamic_partition with hot partitions,
        // storage medium ssd will set all partitions's storage_medium to ssd
        table = 'tbl_dynamic_hot_1'
        sql """CREATE TABLE ${table} (k DATE)
        PARTITION BY RANGE(k)()
        DISTRIBUTED BY HASH (k) BUCKETS 5
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.hot_partition_num" = "1",
            "dynamic_partition.storage_medium" = "ssd",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "3",
            "dynamic_partition.replication_num" = "1",
            "dynamic_partition.create_history_partition"= "true",
            "dynamic_partition.hot"= "true",
            "dynamic_partition.start" = "-4"
        )
        """
        beReportTablet
        checkPartitionMedium table, false

        // test dynamic_partition with hot partitions
        for (def i = 0; i < 2; i++) {
            table = 'tbl_dynamic_hot_2'
            sql "DROP TABLE IF EXISTS ${table} FORCE"
            sql """CREATE TABLE ${table} (k DATE)
            PARTITION BY RANGE(k)()
            DISTRIBUTED BY HASH (k) BUCKETS 5
            PROPERTIES
            (
                "dynamic_partition.storage_medium" = "hdd",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.hot_partition_num" = "1",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "3",
                "dynamic_partition.replication_num" = "1",
                "dynamic_partition.create_history_partition"= "true",
                "dynamic_partition.hot"= "true",
                "dynamic_partition.start" = "-4"
            )
            """

            beReportTablet
            def partitions = sql_return_maparray "SHOW PARTITIONS FROM ${table}"
            if (i == 0 && (partitions.size() != 8 || partitions.get(3).StorageMedium != 'HDD'
                    || partitions.get(4).StorageMedium != 'SSD')) {
                sleep 5000
                continue
            }

            assertEquals(8, partitions.size())
            for (def j = 0; j < 8; j++) {
                assertEquals(j < 4 ? 'HDD' : 'SSD', partitions.get(j).StorageMedium)
            }
        }
    }
}

