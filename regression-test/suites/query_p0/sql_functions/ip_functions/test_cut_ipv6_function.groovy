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
suite("test_cut_ipv6_function") {
    sql """ DROP TABLE IF EXISTS test_cut_ipv6_function """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
    CREATE TABLE `test_cut_ipv6_function` (
      `id` int,
      `ip_v4` string,
      `ip_v6` string,
      `bytes_for_ipv6` tinyint,
      `bytes_for_ipv4` tinyint
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    insert into test_cut_ipv6_function values
    (1, '0.0.0.0', '::', 0, 0),
    (2, '192.168.0.1', '::1', 2, 2),
    (3, '127.0.0.1', '2001:1b70:a1:610::b102:2', 4, 4),
    (4, '255.255.255.254', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe', 8, 8),
    (5, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16, 16),
    (6, NULL, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16, 16),
    (7, '255.255.255.255', NULL, 16, 16),
    (8, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', NULL, 16),
    (9, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16, NULL)
    """

    qt_sql "select cut_ipv6(to_ipv6_or_null(ip_v6), bytes_for_ipv6, 0), cut_ipv6(ipv4_to_ipv6(to_ipv4_or_null(ip_v4)), 0, bytes_for_ipv4) from test_cut_ipv6_function order by id"

    qt_sql "select cut_ipv6(NULL, 0, 0)"
    qt_sql "select cut_ipv6(to_ipv6('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), NULL, 0)"
    qt_sql "select cut_ipv6(to_ipv6('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D'), 0, NULL)"

    sql "DROP TABLE test_cut_ipv6_function"
}