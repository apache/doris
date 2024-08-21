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

suite("test_dynamic_partition_with_update", "nonConcurrent") {
    sql "drop table if exists test_dynamic_partition_with_update"
    sql """
            CREATE TABLE IF NOT EXISTS test_dynamic_partition_with_update
            ( k1 date NOT NULL )
            PARTITION BY RANGE(k1) ( )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
            "dynamic_partition.enable"="true",
            "dynamic_partition.end"="3",
            "dynamic_partition.buckets"="1",
            "dynamic_partition.start"="-3",
            "dynamic_partition.prefix"="p",
            "dynamic_partition.time_unit"="DAY",
            "dynamic_partition.create_history_partition"="true",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "replication_num" = "1");
        """

    // set check interval time
    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '1') """

    // check table init
    def result = sql "show partitions from test_dynamic_partition_with_update"
    assertEquals(7, result.size())
    result = sql "show dynamic partition tables"
    assertEquals("true",result.get(0).get(1))

    // disable dynamic partition to insert partition
    sql """ alter table test_dynamic_partition_with_update set ('dynamic_partition.enable' = 'false') """
    result = sql "show dynamic partition tables"
    assertEquals("false",result.get(0).get(1))

    // manually insert partition
    sql """ alter table test_dynamic_partition_with_update add partition p1 values [("2020-01-02"), ("2020-01-05")) """
    sql """ alter table test_dynamic_partition_with_update add partition p2 values [("2020-05-02"), ("2020-06-06")) """
    sql """ alter table test_dynamic_partition_with_update add partition p3 values [("2020-07-04"), ("2020-07-28")) """
    sql """ alter table test_dynamic_partition_with_update add partition p4 values [("2999-04-25"), ("2999-04-28")) """

    // check size
    result = sql "show partitions from test_dynamic_partition_with_update"
    assertEquals(11, result.size())
    sql """ alter table test_dynamic_partition_with_update set ('dynamic_partition.enable' = 'true') """
    result = sql "show dynamic partition tables"
    assertEquals("true",result.get(0).get(1))

    // check and update
    sleep(3000)

    // check size
    result = sql "show partitions from test_dynamic_partition_with_update"
    assertEquals(8, result.size())

    sql """ admin set frontend config ('dynamic_partition_check_interval_seconds' = '600') """
}
