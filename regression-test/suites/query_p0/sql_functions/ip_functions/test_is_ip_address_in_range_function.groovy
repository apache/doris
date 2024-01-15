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

    sql "insert into test_is_ip_address_in_range_function values(1, '127.0.0.1', '127.0.0.0/8')"
    sql "insert into test_is_ip_address_in_range_function values(2, '128.0.0.1', '127.0.0.0/8')"
    sql "insert into test_is_ip_address_in_range_function values(3, 'ffff::1', 'ffff::/16')"
    sql "insert into test_is_ip_address_in_range_function values(4, 'fffe::1', 'ffff::/16')"
    sql "insert into test_is_ip_address_in_range_function values(5, '192.168.99.255', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(6, '192.168.100.1', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(7, '192.168.103.255', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(8, '192.168.104.0', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(9, '::192.168.99.255', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(10, '::192.168.100.1', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(11, '::192.168.103.255', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(12, '::192.168.104.0', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(13, '192.168.100.1', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(14, '192.168.100.1', '192.168.100.0/24')"
    sql "insert into test_is_ip_address_in_range_function values(15, '192.168.100.1', '192.168.100.0/32')"
    sql "insert into test_is_ip_address_in_range_function values(16, '::192.168.100.1', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(17, '::192.168.100.1', '::192.168.100.0/120')"
    sql "insert into test_is_ip_address_in_range_function values(18, '::192.168.100.1', '::192.168.100.0/128')"
    sql "insert into test_is_ip_address_in_range_function values(19, '192.168.100.1', '192.168.100.0/22')"
    sql "insert into test_is_ip_address_in_range_function values(20, '192.168.103.255', '192.168.100.0/24')"
    sql "insert into test_is_ip_address_in_range_function values(21, '::192.168.100.1', '::192.168.100.0/118')"
    sql "insert into test_is_ip_address_in_range_function values(22, '::192.168.103.255', '::192.168.100.0/120')"
    sql "insert into test_is_ip_address_in_range_function values(23, '127.0.0.1', 'ffff::/16')"
    sql "insert into test_is_ip_address_in_range_function values(24, '127.0.0.1', '::127.0.0.1/128')"
    sql "insert into test_is_ip_address_in_range_function values(25, '::1', '127.0.0.0/8')"
    sql "insert into test_is_ip_address_in_range_function values(26, '::127.0.0.1', '127.0.0.1/32')"

    qt_sql "select id, is_ip_address_in_range(addr, cidr) from test_is_ip_address_in_range_function order by id"

    sql """ DROP TABLE IF EXISTS test_is_ip_address_in_range_function """
}