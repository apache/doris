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

suite("test_partition_default_medium") {
    def options = new ClusterOptions()
    options.feConfigs += [
        'default_storage_medium=HDD',
    ]
    options.beDisks = ['SSD=4']

    def checkCreateTablePartitionDefaultMediumEq = {tbl, sum ->
        sleep 1000

        def partitions = sql_return_maparray "SHOW PARTITIONS FROM $tbl;"
        def partitionsMedium = [:]
        partitions.each {
            def num = partitionsMedium.get(it.StorageMedium)
            if (partitionsMedium) {
                partitionsMedium.put(it.StorageMedium, ++num)
            } else {
                partitionsMedium.put(it.StorageMedium, 1)
            }
        }
        log.info("table ${tbl} partition mediums $partitionsMedium")
        def count = partitionsMedium.values().stream().distinct().count()
        assertEquals(count, 1)
        assertEquals(partitionsMedium.get("SSD"), sum.toInteger())
    }

    docker(options) {
        def single_partition_tbl = "single_partition_tbl"
        def multi_partition_tbl = "multi_partition_tbl"
        def dynamic_partition_tbl = "dynamic_partition_tbl"
        sql """drop table if exists $single_partition_tbl"""
        sql """drop table if exists $multi_partition_tbl"""
        sql """drop table if exists $dynamic_partition_tbl"""

        sql """
            CREATE TABLE ${single_partition_tbl}
            (
                k1 BIGINT,
                k2 LARGEINT,
                v1 VARCHAR(2048),
                v2 SMALLINT DEFAULT "10"
            )
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH (k1, k2) BUCKETS 32;
        """

        checkCreateTablePartitionDefaultMediumEq(single_partition_tbl, 1)


        sql """
            CREATE TABLE $multi_partition_tbl
            (
                k1 DATE,
                k2 DECIMAL(10, 2) DEFAULT "10.5",
                k3 CHAR(10) COMMENT "string column",
                k4 INT NOT NULL DEFAULT "1" COMMENT "int column"
            )
            DUPLICATE KEY(k1, k2)
            COMMENT "my first table"
            PARTITION BY RANGE(k1)
            (
                PARTITION p1 VALUES LESS THAN ("2020-02-01"),
                PARTITION p2 VALUES LESS THAN ("2020-03-01"),
                PARTITION p3 VALUES LESS THAN ("2020-04-01")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 32;
        """
        checkCreateTablePartitionDefaultMediumEq(multi_partition_tbl, 3)

        sql """
            CREATE TABLE $dynamic_partition_tbl
            (
                k1 DATE
            )
            PARTITION BY RANGE(k1) ()
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-7",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "32"
            );
        """
        checkCreateTablePartitionDefaultMediumEq(dynamic_partition_tbl, 4)
    }
}
