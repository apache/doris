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

suite('test_dynamic_partition_small_start', 'nonConcurrent') {
    def old_check_interval = getFeConfig('dynamic_partition_check_interval_seconds')
    try {
        setFeConfig('dynamic_partition_check_interval_seconds', 1);
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_small_start FORCE'
        sql '''CREATE TABLE test_dynamic_partition_small_start
              ( `k1` datetime NULL )
              PARTITION BY RANGE (k1)()
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES
              (
                "replication_num" = "1",
                "dynamic_partition.replication_num" = "1",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.end" = "3",
                "dynamic_partition.time_unit" = "month",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "dynamic_partition.start" = "-100000",
                "dynamic_partition.history_partition_num" = "5",
                "dynamic_partition.create_history_partition" = "true"
              )'''

        def partitions = sql_return_maparray "SHOW PARTITIONS FROM test_dynamic_partition_small_start"
        assertEquals(9, partitions.size());

        sql ''' insert into test_dynamic_partition_small_start values(now()) '''
        sql ''' sync '''

        // wait dynamic partition scheduler, to make sure old partitions will not be dropped
        Thread.sleep(5000);
        qt_sql ''' select count(1) from test_dynamic_partition_small_start '''
    } finally {
        setFeConfig('dynamic_partition_check_interval_seconds', old_check_interval);
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_small_start'
    }
}
