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

suite("test_autobucket") {
    sql "drop table if exists autobucket_test"
    def result = sql """
        CREATE TABLE `autobucket_test` (
          `user_id` largeint(40) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
        """

    result = sql "show create table autobucket_test"
    log.info("show result : ${result}")
    assertTrue(result.toString().containsIgnoreCase("BUCKETS AUTO"))

    result = sql_return_maparray "show partitions from autobucket_test"
    logger.info("${result}")
    // XXX: buckets at pos(8), next maybe impl by sql meta
    // 10 is the default buckets without partition size
    assertEquals(10, Integer.valueOf(result.get(0).Buckets))

    sql "drop table if exists autobucket_test"

    // set min to 5
    sql "ADMIN SET FRONTEND CONFIG ('autobucket_min_buckets' = '5')"
    sql "drop table if exists autobucket_test_min_buckets"
    result = sql """
        CREATE TABLE `autobucket_test_min_buckets` (
          `user_id` largeint(40) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "estimate_partition_size" = "1M"
        )
        """

    result = sql_return_maparray "show partitions from autobucket_test_min_buckets"
    logger.info("${result}")
    // XXX: buckets at pos(8), next maybe impl by sql meta
    assertEquals(5, Integer.valueOf(result.get(0).Buckets))
    // set back to default
    sql "ADMIN SET FRONTEND CONFIG ('autobucket_min_buckets' = '1')"
    sql "drop table if exists autobucket_test_min_buckets"

    // set max to 1
    sql "ADMIN SET FRONTEND CONFIG ('autobucket_max_buckets' = '1')"
    sql "drop table if exists autobucket_test_max_buckets"
    result = sql """
        CREATE TABLE `autobucket_test_max_buckets` (
          `user_id` largeint(40) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "estimate_partition_size" = "100000G"
        )
        """

    result = sql_return_maparray "show partitions from autobucket_test_max_buckets"
    logger.info("${result}")
    // XXX: buckets at pos(8), next maybe impl by sql meta
    assertEquals(1, Integer.valueOf(result.get(0).Buckets)) //equals max bucket
    // set back to default
    sql "ADMIN SET FRONTEND CONFIG ('autobucket_max_buckets' = '128')"
    sql "drop table if exists autobucket_test_max_buckets"
}
