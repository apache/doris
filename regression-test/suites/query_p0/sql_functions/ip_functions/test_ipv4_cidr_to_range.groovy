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

suite("test_ipv4_cidr_to_range") {
    sql """ DROP TABLE IF EXISTS test_ipv4_cidr_to_range """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_ipv4_cidr_to_range (
          `id` int,
          `addr` ipv4,
          `cidr` int
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """
        insert into test_ipv4_cidr_to_range values
        (1, null, 0),
        (2, '127.0.0.1', null),
        (3, '127.0.0.1', 32),
        (4, '127.0.0.1', 24),
        (5, '127.0.0.1', 16),
        (6, '127.0.0.1', 8),
        (7, '127.0.0.1', 0)
        """

    qt_sql "select id, struct_element(ipv4_cidr_to_range(addr, cidr), 'min') as min_range, struct_element(ipv4_cidr_to_range(addr, cidr), 'max') as max_range from test_ipv4_cidr_to_range order by id"
    qt_sql "select id, ipv4_cidr_to_range(addr, 16) from test_ipv4_cidr_to_range order by id;"
    qt_sql """ select id, ipv4_cidr_to_range("127.0.0.1", cidr) from test_ipv4_cidr_to_range order by id;"""
    qt_sql """ select number, ipv4_cidr_to_range("127.0.0.1", 16) from numbers("number"="10") order by number;"""

    sql """ DROP TABLE IF EXISTS test_ipv4_cidr_to_range """

    qt_sql "select ipv4_cidr_to_range(null, 0)"
    qt_sql "select ipv4_cidr_to_range('127.0.0.1', null)"
    qt_sql "select ipv4_cidr_to_range('127.0.0.1', 32)"
    qt_sql "select ipv4_cidr_to_range('127.0.0.1', 16)"
    qt_sql "select ipv4_cidr_to_range('127.0.0.1', 8)"
    qt_sql "select ipv4_cidr_to_range('127.0.0.1', 0)"
}
