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


suite("test_timestamptz_storage_negative_case") {
    sql """
        DROP TABLE IF EXISTS `timestamptz_storage_agg_key_negative_case`;
    """

    test {
        sql """
            CREATE TABLE `timestamptz_storage_agg_key_negative_case` (
              `ts_tz` TIMESTAMPTZ,
              `ts_tz_value` TIMESTAMPTZ sum,
            ) AGGREGATE KEY(`ts_tz`)
            PROPERTIES (
            "replication_num" = "1"
            );
        """
        exception "Aggregate type SUM"
    }
    test {
        sql """
            CREATE TABLE `timestamptz_storage_agg_key_negative_case` (
              `ts_tz` TIMESTAMPTZ,
              `ts_tz_value` TIMESTAMPTZ HLL_UNION,
            ) AGGREGATE KEY(`ts_tz`)
            PROPERTIES (
            "replication_num" = "1"
            );
        """
        exception "Aggregate type HLL_UNION"
    }
    test {
        sql """
            CREATE TABLE `timestamptz_storage_agg_key_negative_case` (
              `ts_tz` TIMESTAMPTZ,
              `ts_tz_value` TIMESTAMPTZ BITMAP_UNION,
            ) AGGREGATE KEY(`ts_tz`)
            PROPERTIES (
            "replication_num" = "1"
            );
        """
        exception "Aggregate type BITMAP_UNION"
    }

    sql """
        CREATE TABLE `timestamptz_storage_agg_key_negative_case` (
          `ts_tz` TIMESTAMPTZ
        ) 
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    // sum
    test {
        sql """ select sum(ts_tz) from timestamptz_storage_agg_key_negative_case; """
        exception "sum requires"
    }
    // avg
    test {
        sql """ select avg(ts_tz) from timestamptz_storage_agg_key_negative_case; """
        exception "avg requires"
    }
}
