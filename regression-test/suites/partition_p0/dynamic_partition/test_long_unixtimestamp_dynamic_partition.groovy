
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

suite("test_long_unixtimestamp_dynamic_partition") {
    // test dynamic partition by using long unix timestamp as partition key
    // table name is dy_par_timestamp_day
    sql "drop table if exists dy_par_timestamp_day"
    sql """
        CREATE TABLE IF NOT EXISTS dy_par_timestamp_day
        (
            k1 bigint
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_source" = "unix_timestamp",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "8",
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    result  = sql "show tables like 'dy_par_timestamp_day'"
    logger.info("${result}")
    assertEquals(result.size(), 1)

    // check parition info
    result = sql_return_maparray "show partitions from dy_par_timestamp_day"
    logger.info("${result}")
    assertEquals(result.size(), 3)

    sql "drop table if exists dy_par_timestamp_day"
}
