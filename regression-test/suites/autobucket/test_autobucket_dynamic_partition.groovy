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

suite("test_autobucket_dynamic_partition") {
    sql "drop table if exists test_autobucket_dynamic_partition"
    def result = sql """
        CREATE TABLE
        test_autobucket_dynamic_partition (k1 DATETIME)
        PARTITION BY
        RANGE (k1) () DISTRIBUTED BY HASH (k1) BUCKETS AUTO
        PROPERTIES (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "WEEK",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "replication_allocation" = "tag.location.default: 1"
        )
        """

    result = sql "show create table test_autobucket_dynamic_partition"
    log.info("show result : ${result}")
    assertTrue(result.toString().containsIgnoreCase("BUCKETS AUTO"))

    result = sql_return_maparray "show partitions from test_autobucket_dynamic_partition"
    logger.info("${result}")
    // XXX: buckets at pos(8), next maybe impl by sql meta
    // 10 is the default buckets without partition size
    assertEquals(result.size(), 3)
    for (def partition : result) {
        assertEquals(Integer.valueOf(partition.Buckets), 10)
    }

    sql "drop table if exists test_autobucket_dynamic_partition"
}
