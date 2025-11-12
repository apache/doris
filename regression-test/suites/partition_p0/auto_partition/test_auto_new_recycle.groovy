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

suite("test_auto_new_recycle", "nonConcurrent") {
    sql "drop table if exists auto_recycle"
    test {
        sql """
            create table auto_recycle(
                k0 datetime(6) not null
            )
            auto partition by range (date_trunc(k0, 'day')) ()
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            properties(
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-1",
                "dynamic_partition.end" = "2",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.create_history_partition" = "true",
                "replication_num" = "1",
                "partition.retention_count" = "3"
            );
        """
        exception "Please remove dynamic_partition properties when partition.retention_count enabled"
    }
    test {
        sql """
            create table auto_recycle(
                k0 datetime(6) not null
            )
            auto partition by list (k0) ()
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "partition.retention_count" = "3"
            );
        """
        exception "Only AUTO RANGE PARTITION table could set partition.retention_count"
    }
    
    sql """
        create table auto_recycle(
            k0 datetime(6) not null
        )
        auto partition by range (date_trunc(k0, 'day')) ()
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties(
            "replication_num" = "1",
            "partition.retention_count" = "3"
        );
    """
    def res = sql "show create table auto_recycle"
    assertTrue(res[0][1].contains('"partition.retention_count" = "3"'))

    sql "drop table auto_recycle force"
    sql """
        create table auto_recycle(
            k0 datetime(6) not null
        )
        auto partition by range (date_trunc(k0, 'day')) ()
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties(
            "replication_num" = "1",
            "partition.retention_count" = "3"
        );
    """
    test {
        sql """
            ALTER TABLE auto_recycle set (
            "dynamic_partition.enable" = "true", 
            "dynamic_partition.time_unit" = "DAY", 
            "dynamic_partition.end" = "3", 
            "dynamic_partition.prefix" = "p", 
            "dynamic_partition.buckets" = "32"
            );
        """
        exception "Can not use partition.retention_count and dynamic_partition properties at the same time"
    }
    test {
        sql "alter table auto_recycle set ('partition.retention_count' = '0')"
        exception "partition.retention_count should be > 0"
    }





    sql "drop table auto_recycle force"
    sql """
        create table auto_recycle(
            k0 datetime(6) not null
        )
        auto partition by range (date_trunc(k0, 'day')) ()
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties(
            "replication_num" = "1"
        );
    """

    waitUntilSafeExecutionTime("NOT_CROSS_DAY_BOUNDARY", 20)

    sql """
    insert into auto_recycle select date_add('2020-01-01 00:00:00', interval number day) from numbers("number" = "100");
    """
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 100)

    sql "alter table auto_recycle set ('partition.retention_count' = '3')"
    res = sql "show create table auto_recycle"
    assertTrue(res[0][1].contains('"partition.retention_count" = "3"'))

    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(8000)
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 3)

    // before everytime insert data, we should avoid recycle because if they conflict, insert will fail
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
    sleep(8000)
    sql """
    insert into auto_recycle select date_add('2020-01-01 00:00:00', interval number day) from numbers("number" = "100");
    """
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(8000)
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 3)
    qt_sql0 "select * from auto_recycle order by k0"

    sql "alter table auto_recycle set ('partition.retention_count' = '1')"
    sleep(5000)
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 1)

    sql "alter table auto_recycle set ('partition.retention_count' = '10')"
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
    sleep(8000)
    sql """
    insert into auto_recycle select date_add('2022-01-01 00:00:00', interval number day) from numbers("number" = "100");
    """
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(8000)
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 10)
    qt_sql1 "select * from auto_recycle order by k0"

    // verify not consider future partition when do recycle
    sql "drop table auto_recycle force"
    sql """
        CREATE TABLE auto_recycle (
            k0 DATETIME NOT NULL
        )
        partition by range (date_trunc(k0, 'day')) ()
        PROPERTIES (
            "replication_num" = "1",
            "partition.retention_count" = "3"
        );
    """
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
    sleep(8000)
    sql """
        insert into auto_recycle select date_add(now(), interval number-5 day) from numbers("number" = "8");
    """
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """
    sleep(8000)
    res = sql "show partitions from auto_recycle"
    assertEquals(res.size(), 6) // [-5, -1] -> [-3, -1], [0, 2] -> [0, 2]
    res = sql "select * from auto_recycle order by k0"
    assertEquals(res.size(), 6)

    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
}