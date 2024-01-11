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
suite("test_to_ip_functions") {
    sql """ DROP TABLE IF EXISTS test_to_ip_functions """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
    CREATE TABLE `test_to_ip_functions` (
      `id` int,
      `ip_v4` string,
      `ip_v6` string
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "insert into test_to_ip_functions values(0, NULL, NULL)"
    sql "insert into test_to_ip_functions values(1, '0.0.0.0', '::')"
    sql "insert into test_to_ip_functions values(2, '192.168.0.1', '::1')"
    sql "insert into test_to_ip_functions values(3, '127.0.0.1', '2001:1b70:a1:610::b102:2')"
    sql "insert into test_to_ip_functions values(4, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')"

    qt_sql "select to_ipv4(ip_v4), to_ipv6(ip_v6) from test_to_ip_functions order by id"

    sql "DROP TABLE test_to_ip_functions"
}