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

suite("test_auto_dynamic", "nonConcurrent") {
    // PROHIBIT different timeunit of interval when use both auto & dynamic partition
    sql " drop table if exists tbl3 "
    test{
        sql """
            CREATE TABLE tbl3
            (
                k1 DATETIME NOT NULL,
                col1 int 
            )
            auto partition by range (date_trunc(`k1`, 'year')) ()
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "replication_num" = "1",
                "dynamic_partition.create_history_partition"="true",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "HOUR",
                "dynamic_partition.start" = "-2",
                "dynamic_partition.end" = "2",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "8"
            ); 
        """
        exception "If support auto partition and dynamic partition at same time, they must have the same interval unit."
    }

    sql " drop table if exists test_dynamic "
    sql """
            create table test_dynamic(
                k0 DATE not null
            )
            auto partition by range (date_trunc(k0, 'year')) ()
            DISTRIBUTED BY HASH(`k0`) BUCKETS auto
            properties("replication_num" = "1");
        """
    test {
        sql """
            ALTER TABLE test_dynamic set (
                "dynamic_partition.enable" = "true", 
                "dynamic_partition.time_unit" = "DAY", 
                "dynamic_partition.end" = "3", 
                "dynamic_partition.prefix" = "p", 
                "dynamic_partition.buckets" = "32"
            );
        """
        exception "If support auto partition and dynamic partition at same time, they must have the same interval unit."
    }
    sql """
        ALTER TABLE test_dynamic set (
            "dynamic_partition.enable" = "true", 
            "dynamic_partition.time_unit" = "YeAr", 
            "dynamic_partition.end" = "3", 
            "dynamic_partition.prefix" = "p", 
            "dynamic_partition.buckets" = "32"
        );
    """

    sql " drop table if exists auto_dynamic "
    sql """
        create table auto_dynamic(
            k0 datetime(6) NOT NULL
        )
        auto partition by range (date_trunc(k0, 'hour'))
        (
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 2
        properties(
            "dynamic_partition.enable" = "true",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.create_history_partition" = "true",
            "dynamic_partition.start" = "-5",
            "dynamic_partition.end" = "0",
            "dynamic_partition.time_unit" = "hour",
            "replication_num" = "1"
        );
    """
    def part_result = sql " show partitions from auto_dynamic "
    assertEquals(part_result.size(), 6)

    sql " drop table if exists auto_dynamic "
    sql """
        create table auto_dynamic(
            k0 datetime(6) NOT NULL
        )
        auto partition by range (date_trunc(k0, 'year'))
        (
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 2
        properties(
            "dynamic_partition.enable" = "true",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.start" = "-50",
            "dynamic_partition.end" = "0",
            "dynamic_partition.time_unit" = "year",
            "replication_num" = "1"
        );
    """
    part_result = sql " show partitions from auto_dynamic "
    assertEquals(part_result.size(), 1)

    def oldCheckInterval = getFeConfig("dynamic_partition_check_interval_seconds")
    try {
        def insertException = null
        test {
            sql " insert into auto_dynamic values ('2024-01-01'), ('2900-01-01'), ('1900-01-01'), ('3000-01-01'); "
            check { result, exception, startTime, endTime ->
                insertException = exception
            }
        }
        if (insertException != null) {
            part_result = sql " show partitions from auto_dynamic "
            def partitionNames = part_result.collect { it[1] }
            def expectedSurvivingPartitions = ["p20240101000000", "p29000101000000", "p30000101000000"]
            // A stale short-interval run can only delete the out-of-range p1900 partition before commit.
            assertTrue(!partitionNames.contains("p19000101000000")
                && partitionNames.containsAll(expectedSurvivingPartitions),
                "Unexpected insert failure: ${insertException}, partitions: ${partitionNames}")
            return true
        }

        setFeConfig("dynamic_partition_check_interval_seconds", 1)
        def partitionNames = []
        for (int retry = 0; retry < 1200; retry++) {
            part_result = sql " show partitions from auto_dynamic "
            partitionNames = part_result.collect { it[1] }
            if (!partitionNames.contains("p19000101000000")) {
                break
            }
            sleep(1000)
        }
        // Changing the interval does not wake a scheduler that is already sleeping.
        assertTrue(!partitionNames.contains("p19000101000000"),
            "Dynamic partition scheduler did not recycle p1900 within 1200 seconds, partitions: ${partitionNames}")
        log.info("${part_result}".toString())
        assertTrue(part_result.size() == 3 || part_result.size() == 4,
            "The partition size should be 3 or 4, but got ${part_result.size()}")

        qt_sql_dynamic_auto "select * from auto_dynamic order by k0;"
    } finally {
        setFeConfig("dynamic_partition_check_interval_seconds", oldCheckInterval)
    }
}
