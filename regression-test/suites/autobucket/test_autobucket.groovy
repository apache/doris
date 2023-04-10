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
    result = sql """
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
    assertTrue(result.toString().containsIgnoreCase("BUCKETS AUTO"))

    result = sql "show partitions from autobucket_test"
    logger.info("${result}")
    // XXX: buckets at pos(8), next maybe impl by sql meta
    // 10 is the default buckets without partition size
    assertEquals(Integer.valueOf(result.get(0).get(8)), 10)

    sql "drop table if exists autobucket_test"
}
