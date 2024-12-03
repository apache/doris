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

suite("test_ipv6_cidr_to_range_function") {
    sql """ DROP TABLE IF EXISTS test_ipv6_cidr_to_range_function """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_ipv6_cidr_to_range_function (
          `id` int,
          `addr` ipv6,
          `cidr` int
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """
        insert into test_ipv6_cidr_to_range_function values
        (1, '2001:0db8:0000:85a3:0000:0000:ac1f:8001', 0),
        (2, '2001:0db8:0000:85a3:ffff:ffff:ffff:ffff', 32),
        (3, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16),
        (4, '2001:df8:0:85a3::ac1f:8001', 32),
        (5, '2001:0db8:85a3:85a3:0000:0000:ac1f:8001', 16),
        (6, '0000:0000:0000:0000:0000:0000:0000:0000', 8),
        (7, 'ffff:0000:0000:0000:0000:0000:0000:0000', 4),
        (8, NULL, 8),
        (9, 'ffff:0000:0000:0000:0000:0000:0000:0000', NULL)
        """

    qt_sql "select id, struct_element(ipv6_cidr_to_range(addr, cidr), 'min') as min_range, struct_element(ipv6_cidr_to_range(addr, cidr), 'max') as max_range from test_ipv6_cidr_to_range_function order by id"
    qt_sql "select id, ipv6_cidr_to_range(addr, 16) from test_ipv6_cidr_to_range_function order by id;"
    sql """ delete from test_ipv6_cidr_to_range_function where id in (2,3,6);"""
    test {
       sql """select id, ipv6_cidr_to_range(to_ipv6("127.0.0.1"), cidr) from test_ipv6_cidr_to_range_function order by id;"""
      exception "Invalid IPv6 value '127.0.0.1'"
    }
    test {
     sql """ select number, ipv6_cidr_to_range(to_ipv6("127.0.0.1"), 16) from numbers("number"="10") order by number;"""
     exception "Invalid IPv6 value '127.0.0.1'"
   }
   qt_sql """ select id, ipv6_cidr_to_range(to_ipv6("::1"), cidr) from test_ipv6_cidr_to_range_function order by id; """
   qt_sql """ select number, ipv6_cidr_to_range(to_ipv6("::1"), 16) from numbers("number"="10") order by number;"""
     

    sql """ DROP TABLE IF EXISTS test_ipv6_cidr_to_range_function """
    sql """ DROP TABLE IF EXISTS test_str_cidr_to_range_function """

    sql """
        CREATE TABLE test_str_cidr_to_range_function (
          `id` int,
          `addr` string,
          `cidr` int
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """
        insert into test_str_cidr_to_range_function values
        (1, '2001:0db8:0000:85a3:0000:0000:ac1f:8001', 0),
        (2, '2001:0db8:0000:85a3:ffff:ffff:ffff:ffff', 32),
        (3, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16),
        (4, '2001:df8:0:85a3::ac1f:8001', 32),
        (5, '2001:0db8:85a3:85a3:0000:0000:ac1f:8001', 16),
        (6, '0000:0000:0000:0000:0000:0000:0000:0000', 8),
        (7, 'ffff:0000:0000:0000:0000:0000:0000:0000', 4),
        (8, NULL, 8),
        (9, 'ffff:0000:0000:0000:0000:0000:0000:0000', NULL)
        """

    qt_sql "select id, struct_element(ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num_or_null(addr)), cidr), 'min') as min_range, struct_element(ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num_or_null(addr)), cidr), 'max') as max_range from test_str_cidr_to_range_function order by id"

    sql """ DROP TABLE IF EXISTS test_str_cidr_to_range_function """

    qt_sql "select ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001')), 0)"
    qt_sql "select ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001')), 128)"
    qt_sql "select ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')), 64)"
    qt_sql "select ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num('0000:0000:0000:0000:0000:0000:0000:0000')), 8)"
    qt_sql "select ipv6_cidr_to_range(ipv6_num_to_string(ipv6_string_to_num('ffff:0000:0000:0000:0000:0000:0000:0000')), 4)"
}
