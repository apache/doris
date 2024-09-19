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
    assertEquals(part_result.size, 6)

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
    assertEquals(part_result.size, 1)

    def skip_test = false
    test {
        sql " insert into auto_dynamic values ('2024-01-01'), ('2900-01-01'), ('1900-01-01'), ('3000-01-01'); "
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                // the partition of 1900-01-01 directly been recovered before the insert txn finished. let it success
                part_result = sql " show partitions from auto_dynamic "
                log.info("${part_result}".toString())
                assertTrue(exception.getMessage().contains("get partition p19000101000000 failed"))
                skip_test = true
            }
        }
    }
    if (skip_test) {
        return true
    }

    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(2000)
    part_result = sql " show partitions from auto_dynamic "
    log.info("${part_result}".toString())
    assertEquals(part_result.size, 3)

    qt_sql_dynamic_auto "select * from auto_dynamic order by k0;"

    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
}