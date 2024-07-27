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

suite("test_ipv6_num_to_string_function") {
    sql """ DROP TABLE IF EXISTS test_ipv6_num_to_string_function """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_ipv6_num_to_string_function (
          `id` int,
          `ip_v6` ipv6,
          `ip_v6_str` string
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """
        insert into test_ipv6_num_to_string_function values
        (1, '::', '::'),
        (2, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        (3, 'ef8d:3d6a:869b:2582:7200:aa46:4dcd:2bd4', 'ef8d:3d6a:869b:2582:7200:aa46:4dcd:2bd4'),
        (4, '4a3e:dc26:1819:83e6:9ee5:7239:ff44:aee8', '4a3e:dc26:1819:83e6:9ee5:7239:ff44:aee8'),
        (5, 'b374:22dc:814a:567b:6013:78a7:032d:05c8', 'b374:22dc:814a:567b:6013:78a7:032d:05c8'),
        (6, '1326:d47e:2417:83c0:bd35:fc82:34dc:953a', '1326:d47e:2417:83c0:bd35:fc82:34dc:953a'),
        (7, '8ffa:65cb:6554:5c3e:fb87:3f91:29da:2891', '8ffa:65cb:6554:5c3e:fb87:3f91:29da:2891'),
        (8, 'def7:1488:6fb7:0c70:aa66:df25:6a43:5d89', 'def7:1488:6fb7:0c70:aa66:df25:6a43:5d89'),
        (9, 'd3fa:09a9:af08:0c8b:44ab:8f75:0b11:e997', 'd3fa:09a9:af08:0c8b:44ab:8f75:0b11:e997'),
        (10, NULL, NULL);
        """

    qt_sql "select id, ipv6_num_to_string(ip_v6), ipv6_num_to_string(ipv6_string_to_num_or_null(ip_v6_str)) from test_ipv6_num_to_string_function order by id"

    sql """ DROP TABLE IF EXISTS test_ipv6_num_to_string_function """
}