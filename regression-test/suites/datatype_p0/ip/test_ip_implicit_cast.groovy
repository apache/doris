
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

suite("test_ip_implicit_cast") {
    def tableName = "test_ip_implicit_cast"
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
    CREATE TABLE ${tableName} (
      `id` bigint,
      `ip_v4` ipv4,
      `ip_v6` ipv6
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "insert into ${tableName} values(-1, NULL, NULL)"
    sql "insert into ${tableName} values(0, 0, '::')"
    sql "insert into ${tableName} values(1, 1, '::1')"
    sql "insert into ${tableName} values(2130706433, 2130706433, '2001:1b70:a1:610::b102:2')"
    sql "insert into ${tableName} values(4294967295, 4294967295, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')"

    qt_sql1 "select id, ip_v4 from ${tableName} order by id"

    sql "DROP TABLE ${tableName}"
}
