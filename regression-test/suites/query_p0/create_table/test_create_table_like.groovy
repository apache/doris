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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("test_create_table_like") {
    multi_sql """
    DROP TABLE IF EXISTS test_create_table_like_t1;

    CREATE TABLE `test_create_table_like_t1` (
    `id` int NULL,
    `m_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    DROP TABLE IF EXISTS test_create_table_like_t2;

    CREATE TABLE test_create_table_like_t2 like test_create_table_like_t1;
    """
}
