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

suite('test_dynamic_partition_failed', 'nonConcurrent') {
    def old_max_dynamic_partition_num = getFeConfig('max_dynamic_partition_num')
    try {
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_ok1 FORCE'
        sql '''CREATE TABLE test_dynamic_partition_failed_ok1
              ( `k1` datetime NULL )
              PARTITION BY RANGE (k1)()
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES
              (
                "replication_num" = "1",
                "dynamic_partition.replication_num" = "1",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.end" = "3",
                "dynamic_partition.time_unit" = "day",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "dynamic_partition.start" = "-99999999",
                "dynamic_partition.history_partition_num" = "5",
                "dynamic_partition.create_history_partition" = "true"
              )'''

        def partitions = sql_return_maparray "SHOW PARTITIONS FROM test_dynamic_partition_failed_ok1"
        assertEquals(9, partitions.size());
        def dynamicInfo = sql_return_maparray("SHOW DYNAMIC PARTITION TABLES").find { it.TableName == 'test_dynamic_partition_failed_ok1' }
        logger.info("table dynamic info: " + dynamicInfo)
        assertNotNull(dynamicInfo)
        assertTrue(dynamicInfo.LastDropPartitionMsg.contains("'dynamic_partition.start' = -99999999, maybe it's too small, "
                + "can use alter table sql to increase it."))

        setFeConfig('max_dynamic_partition_num', Integer.MAX_VALUE)

        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_ok2 FORCE'
        sql '''CREATE TABLE test_dynamic_partition_failed_ok2
              ( `k1` date NULL )
              PARTITION BY RANGE(k1) (
                 PARTITION `phistory` VALUES less than ('2020-01-01')
              )
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES
              (
                "replication_num" = "1",
                "dynamic_partition.replication_num" = "1",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.end" = "3",
                "dynamic_partition.time_unit" = "YEAR",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "dynamic_partition.start" = "-10",
                "dynamic_partition.create_history_partition" = "true"
              )'''

        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_2'
        test {
            sql '''CREATE TABLE test_dynamic_partition_failed_2
                  ( `k1` datetime NULL )
                  PARTITION BY RANGE (k1)()
                  DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                  PROPERTIES
                  (
                    "replication_num" = "1",
                    "dynamic_partition.replication_num" = "1",
                    "dynamic_partition.enable" = "true",
                    "dynamic_partition.end" = "3",
                    "dynamic_partition.time_unit" = "day",
                    "dynamic_partition.prefix" = "p",
                    "dynamic_partition.buckets" = "1",
                    "dynamic_partition.start" = "-99999999",
                    "dynamic_partition.create_history_partition" = "true"
                  )'''
            check { result, exception, startTime, endTime ->
                assertNotNull(exception)
                def msg = exception.toString()
                logger.info("exception: " + msg)
                // 'date/datetime literal [+271768-09-11 00:00:00] is invalid'
                assertTrue(msg.contains('date/datetime literal') && msg.contains('is invalid'))
            }

        }

        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_3'
        test {
            sql '''CREATE TABLE test_dynamic_partition_failed_3
                  ( `k1` datetime NULL )
                  PARTITION BY RANGE (k1)()
                  DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                  PROPERTIES
                  (
                    "replication_num" = "1",
                    "dynamic_partition.enable" = "true",
                    "dynamic_partition.end" = "3",
                    "dynamic_partition.time_uint" = "day",
                    "dynamic_partition.prefix" = "p",
                    "dynamic_partition.buckets" = "1",
                    "dynamic_partition.start" = "2024-06-11",
                    "dynamic_partition.edn" = "2024-06-13",
                    "dynamic_partition.create_history_partition" = "true"
                  )'''
            check { result, exception, startTime, endTime ->
                assertNotNull(exception)
                def msg = exception.toString()
                logger.info("exception: " + msg)
                // 'Invalid dynamic partition properties: dynamic_partition.time_uint, dynamic_partition.edn'
                assertTrue(msg.contains('Invalid dynamic partition properties: dynamic_partition.time_uint, dynamic_partition.edn'))
            }
        }
    } finally {
        setFeConfig('max_dynamic_partition_num', old_max_dynamic_partition_num)
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_ok1 FORCE'
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_ok2 FORCE'
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_2'
        sql 'DROP TABLE IF EXISTS test_dynamic_partition_failed_3'
    }
}
