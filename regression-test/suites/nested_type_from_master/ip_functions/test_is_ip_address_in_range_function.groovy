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

suite("test_is_ip_address_in_range_function") {
    sql """ DROP TABLE IF EXISTS test_is_ip_address_in_range_function """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_is_ip_address_in_range_function (
          `id` int,
          `addr` string,
          `cidr` string
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """
        insert into test_is_ip_address_in_range_function values
        (1, '127.0.0.1', '127.0.0.0/8'),
        (2, '128.0.0.1', '127.0.0.0/8'),
        (3, 'ffff::1', 'ffff::/16'),
        (4, 'fffe::1', 'ffff::/16'),
        (5, '192.168.99.255', '192.168.100.0/22'),
        (6, '192.168.100.1', '192.168.100.0/22'),
        (7, '192.168.103.255', '192.168.100.0/22'),
        (8, '192.168.104.0', '192.168.100.0/22'),
        (9, '::192.168.99.255', '::192.168.100.0/118'),
        (10, '::192.168.100.1', '::192.168.100.0/118'),
        (11, '::192.168.103.255', '::192.168.100.0/118'),
        (12, '::192.168.104.0', '::192.168.100.0/118'),
        (13, '192.168.100.1', '192.168.100.0/22'),
        (14, '192.168.100.1', '192.168.100.0/24'),
        (15, '192.168.100.1', '192.168.100.0/32'),
        (16, '::192.168.100.1', '::192.168.100.0/118'),
        (17, '::192.168.100.1', '::192.168.100.0/120'),
        (18, '::192.168.100.1', '::192.168.100.0/128'),
        (19, '192.168.100.1', '192.168.100.0/22'),
        (20, '192.168.103.255', '192.168.100.0/24'),
        (21, '::192.168.100.1', '::192.168.100.0/118'),
        (22, '::192.168.103.255', '::192.168.100.0/120'),
        (23, '127.0.0.1', 'ffff::/16'),
        (24, '127.0.0.1', '::127.0.0.1/128'),
        (25, '::1', '127.0.0.0/8'),
        (26, '::127.0.0.1', '127.0.0.1/32')
        """

    // vector vs vector
    qt_sql "select id, is_ip_address_in_range(addr, cidr) from test_is_ip_address_in_range_function order by id"

    // vector vs scalar
    qt_sql "select id, is_ip_address_in_range(addr, '192.168.100.0/24') from test_is_ip_address_in_range_function order by id"

    // scalar vs vector
    qt_sql "select id, is_ip_address_in_range('192.168.100.0', cidr) from test_is_ip_address_in_range_function order by id"

    qt_sql "SELECT is_ip_address_in_range('::ffff:192.168.0.1', NULL)"

    qt_sql "SELECT is_ip_address_in_range(NULL, '::ffff:192.168.0.4/128')"

    qt_sql "SELECT is_ip_address_in_range(NULL, NULL)"
}