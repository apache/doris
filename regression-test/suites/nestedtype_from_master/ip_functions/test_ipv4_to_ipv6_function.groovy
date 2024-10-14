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
suite("test_ipv4_to_ipv6_function") {
    sql """ DROP TABLE IF EXISTS test_ipv4_to_ipv6_function """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
    CREATE TABLE `test_ipv4_to_ipv6_function` (
      `id` int,
      `ip_v4` string
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    insert into test_ipv4_to_ipv6_function values
    (1, '0.0.0.0'),
    (2, '0.0.0.1'),
    (3, '127.0.0.1'),
    (4, '192.168.0.1'),
    (5, '255.255.255.254'),
    (6, '255.255.255.255'),
    (7, NULL)
    """

    qt_sql "select ipv6_num_to_string(ipv4_to_ipv6(to_ipv4_or_null(ip_v4))) from test_ipv4_to_ipv6_function order by id"

    qt_sql """ select ipv6_num_to_string(ipv4_to_ipv6(to_ipv4('192.168.0.1'))) """

    qt_sql "select ipv4_to_ipv6(NULL)"

    sql "DROP TABLE test_ipv4_to_ipv6_function"
}