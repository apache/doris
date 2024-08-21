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

suite("test_datetime_overflow") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql """drop table if exists datetime_overflow_t"""
    sql """CREATE TABLE datetime_overflow_t (
            `id` bigint NULL,
            `c` datetime NULL,
            `d` date NULL,
            INDEX idx_c (`c`) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS AUTO
            PROPERTIES (
            "replication_num" = "1"
            );"""

    sql """select * from datetime_overflow_t where d between "9999-12-31 00:00:01" and "9999-12-31 10:00:01";"""
    sql """select * from datetime_overflow_t where d > "9999-12-31 00:00:01";"""
}
